-- Tax Processing Database Schema for PostgreSQL
-- Optimized for high-performance batch processing with proper indexing

-- Set search path
SET search_path TO tax, public;

-- Opening Balances Table
-- Stores the starting positions for each account-instrument combination for a tax year
CREATE TABLE IF NOT EXISTS tax.opening_balances (
    id BIGINT DEFAULT nextval('tax.opening_balances_id_seq') PRIMARY KEY,
    account_id VARCHAR(50) NOT NULL,
    instrument VARCHAR(20) NOT NULL,
    tax_year VARCHAR(4) NOT NULL,
    units_held NUMERIC(18,6) NOT NULL,
    book_cost NUMERIC(18,2) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,

    -- Unique constraint to prevent duplicate positions
    CONSTRAINT uq_opening_balances_account_instrument_tax_year
        UNIQUE (account_id, instrument, tax_year)
);

-- Indexes for opening_balances
-- Clustered-equivalent index for efficient windowed queries (tax_year, id)
CREATE INDEX IF NOT EXISTS ix_opening_balances_tax_year_id
    ON tax.opening_balances (tax_year, id);

-- Index for position lookups
CREATE INDEX IF NOT EXISTS ix_opening_balances_position
    ON tax.opening_balances (account_id, instrument);

-- Transactions Table
-- Stores all transactions that affect positions (buys, sells, dividends, etc.)
CREATE TABLE IF NOT EXISTS tax.transactions (
    id BIGINT DEFAULT nextval('tax.transactions_id_seq') NOT NULL,
    transaction_id VARCHAR(50) NOT NULL, -- External transaction ID for idempotency
    account_id VARCHAR(50) NOT NULL,
    instrument VARCHAR(20) NOT NULL,
    tax_year VARCHAR(4) NOT NULL,
    transaction_date TIMESTAMPTZ NOT NULL,
    transaction_type VARCHAR(20) NOT NULL, -- BUY, SELL, DIVIDEND, SPLIT, etc.
    units NUMERIC(18,6) NOT NULL, -- Positive for buys, negative for sells
    price_per_unit NUMERIC(18,6), -- Price per unit (null for splits/dividends)
    total_amount NUMERIC(18,2) NOT NULL, -- Total transaction amount
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,

    -- Primary key
    PRIMARY KEY (id, tax_year),

    -- Unique constraint on external transaction ID within tax year
    CONSTRAINT uq_transactions_transaction_id_tax_year
        UNIQUE (transaction_id, tax_year)
);

-- Indexes for transactions
-- Primary index optimized for position-based queries with chronological processing
CREATE INDEX IF NOT EXISTS ix_transactions_position_date
    ON tax.transactions (account_id, instrument, tax_year, transaction_date, id);

-- Index for efficient windowed loading by position batch
CREATE INDEX IF NOT EXISTS ix_transactions_tax_year_position
    ON tax.transactions (tax_year, account_id, instrument, transaction_date);

-- Index for transaction ID lookups (idempotency checks)
CREATE INDEX IF NOT EXISTS ix_transactions_transaction_id
    ON tax.transactions (transaction_id);

-- Position Summary Table (Optional - for reporting/validation)
-- Maintains current position state after processing
CREATE TABLE IF NOT EXISTS tax.position_summary (
    id BIGINT DEFAULT nextval('tax.position_summary_id_seq') PRIMARY KEY,
    account_id VARCHAR(50) NOT NULL,
    instrument VARCHAR(20) NOT NULL,
    tax_year VARCHAR(4) NOT NULL,
    units_held NUMERIC(18,6) NOT NULL,
    book_cost NUMERIC(18,2) NOT NULL,
    realized_gain_loss NUMERIC(18,2) DEFAULT 0,
    last_processed_transaction_id BIGINT,
    last_updated TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,

    -- Unique constraint for position lookups
    CONSTRAINT uq_position_summary_position_tax_year
        UNIQUE (account_id, instrument, tax_year)
);

-- Indexes for position_summary
CREATE INDEX IF NOT EXISTS ix_position_summary_tax_year
    ON tax.position_summary (tax_year);

-- Processing Status Table
-- Tracks batch processing status and progress
CREATE TABLE IF NOT EXISTS tax.processing_status (
    batch_id VARCHAR(100) PRIMARY KEY,
    tax_year VARCHAR(4) NOT NULL,
    status VARCHAR(20) NOT NULL, -- PENDING, INITIALIZING, PROCESSING, COMPLETED, FAILED
    total_positions BIGINT,
    processed_positions BIGINT DEFAULT 0,
    total_windows INTEGER,
    processed_windows INTEGER DEFAULT 0,
    start_time TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMPTZ,
    error_message TEXT
);

-- Indexes for processing_status
CREATE INDEX IF NOT EXISTS ix_processing_status_tax_year
    ON tax.processing_status (tax_year);

CREATE INDEX IF NOT EXISTS ix_processing_status_status
    ON tax.processing_status (status);

-- Views for common queries

-- View for positions with their opening balances
CREATE OR REPLACE VIEW tax.positions_with_opening_balances AS
SELECT
    ob.account_id,
    ob.instrument,
    ob.tax_year,
    ob.units_held as opening_units,
    ob.book_cost as opening_book_cost,
    COALESCE(ps.units_held, ob.units_held) as current_units,
    COALESCE(ps.book_cost, ob.book_cost) as current_book_cost,
    COALESCE(ps.realized_gain_loss, 0) as realized_gain_loss,
    ps.last_updated
FROM tax.opening_balances ob
LEFT JOIN tax.position_summary ps
    ON ob.account_id = ps.account_id
    AND ob.instrument = ps.instrument
    AND ob.tax_year = ps.tax_year;

-- View for transaction counts by position (useful for debugging)
CREATE OR REPLACE VIEW tax.transaction_counts_by_position AS
SELECT
    account_id,
    instrument,
    tax_year,
    COUNT(*) as transaction_count,
    MIN(transaction_date) as first_transaction,
    MAX(transaction_date) as last_transaction,
    SUM(CASE WHEN transaction_type = 'BUY' THEN units ELSE 0 END) as total_buy_units,
    SUM(CASE WHEN transaction_type = 'SELL' THEN ABS(units) ELSE 0 END) as total_sell_units
FROM tax.transactions
GROUP BY account_id, instrument, tax_year;

-- Create functions for batch operations

-- Function to get opening balances for a specific window
CREATE OR REPLACE FUNCTION tax.get_opening_balances_window(
    p_tax_year VARCHAR(4),
    p_offset INTEGER,
    p_batch_size INTEGER
)
RETURNS TABLE (
    account_id VARCHAR(50),
    instrument VARCHAR(20),
    units_held NUMERIC(18,6),
    book_cost NUMERIC(18,2)
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        ob.account_id,
        ob.instrument,
        ob.units_held,
        ob.book_cost
    FROM tax.opening_balances ob
    WHERE ob.tax_year = p_tax_year
    ORDER BY ob.id
    OFFSET p_offset
    LIMIT p_batch_size;
END;
$$ LANGUAGE plpgsql;

-- Function to get transactions for a list of positions
CREATE OR REPLACE FUNCTION tax.get_transactions_for_positions(
    p_tax_year VARCHAR(4),
    p_positions TEXT[], -- Array of 'account_id|instrument' strings
    p_offset INTEGER,
    p_batch_size INTEGER
)
RETURNS TABLE (
    transaction_id VARCHAR(50),
    account_id VARCHAR(50),
    instrument VARCHAR(20),
    transaction_date TIMESTAMPTZ,
    transaction_type VARCHAR(20),
    units NUMERIC(18,6),
    price_per_unit NUMERIC(18,6),
    total_amount NUMERIC(18,2)
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        t.transaction_id,
        t.account_id,
        t.instrument,
        t.transaction_date,
        t.transaction_type,
        t.units,
        t.price_per_unit,
        t.total_amount
    FROM tax.transactions t
    WHERE t.tax_year = p_tax_year
    AND (t.account_id || '|' || t.instrument) = ANY(p_positions)
    ORDER BY t.account_id, t.instrument, t.transaction_date, t.id
    OFFSET p_offset
    LIMIT p_batch_size;
END;
$$ LANGUAGE plpgsql;

-- Function to count total opening balances for a tax year
CREATE OR REPLACE FUNCTION tax.count_opening_balances(
    p_tax_year VARCHAR(4)
)
RETURNS BIGINT AS $$
BEGIN
    RETURN (
        SELECT COUNT(*)
        FROM tax.opening_balances
        WHERE tax_year = p_tax_year
    );
END;
$$ LANGUAGE plpgsql;

-- Add helpful comments
COMMENT ON TABLE tax.opening_balances IS 'Starting positions for each account-instrument combination by tax year';
COMMENT ON TABLE tax.transactions IS 'All transactions affecting positions (buys, sells, dividends, etc.)';
COMMENT ON TABLE tax.position_summary IS 'Current position state after processing (optional)';
COMMENT ON TABLE tax.processing_status IS 'Batch processing progress tracking';

COMMENT ON FUNCTION tax.get_opening_balances_window IS 'Retrieves a window of opening balances for batch processing';
COMMENT ON FUNCTION tax.get_transactions_for_positions IS 'Retrieves transactions for a specific set of positions';
COMMENT ON FUNCTION tax.count_opening_balances IS 'Counts total opening balances for a tax year';

\echo 'Tax Processing PostgreSQL schema created successfully.';
\echo 'Tables: opening_balances, transactions, position_summary, processing_status';
\echo 'Views: positions_with_opening_balances, transaction_counts_by_position';
\echo 'Functions: get_opening_balances_window, get_transactions_for_positions, count_opening_balances';