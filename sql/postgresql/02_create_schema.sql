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

-- Add helpful comments
COMMENT ON TABLE tax.opening_balances IS 'Starting positions for each account-instrument combination by tax year';
COMMENT ON TABLE tax.transactions IS 'All transactions affecting positions (buys, sells, dividends, etc.)';
\echo 'Tax Processing PostgreSQL schema created successfully.';
\echo 'Tables: opening_balances, transactions';