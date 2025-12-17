-- Generate Configurable Test Data for Tax Processing System (PostgreSQL Version)
-- Usage: Modify the configuration variables below to generate different test scenarios

-- Set search path
SET search_path TO tax, public;

-- ========================================
-- CONFIGURATION SECTION - MODIFY THESE VALUES
-- ========================================

-- Configuration as PostgreSQL variables
\set tax_year '2023'
\set opening_balance_count 100000
\set min_transactions_per_position 0
\set max_transactions_per_position 8

-- Value Ranges
\set min_units_held 10
\set max_units_held 10000
\set min_price_per_unit 5
\set max_price_per_unit 500

-- Batch Processing Configuration
\set batch_size 10000

-- ========================================
-- UTILITY FUNCTIONS
-- ========================================

-- Function to generate random number in range
CREATE OR REPLACE FUNCTION random_between(min_val NUMERIC, max_val NUMERIC)
RETURNS NUMERIC AS $$
BEGIN
    RETURN min_val + (RANDOM() * (max_val - min_val));
END;
$$ LANGUAGE plpgsql;

-- Function to generate random integer in range
CREATE OR REPLACE FUNCTION random_int_between(min_val INTEGER, max_val INTEGER)
RETURNS INTEGER AS $$
BEGIN
    RETURN min_val + FLOOR(RANDOM() * (max_val - min_val + 1))::INTEGER;
END;
$$ LANGUAGE plpgsql;

-- Function to generate weighted random transaction count
CREATE OR REPLACE FUNCTION get_transaction_count()
RETURNS INTEGER AS $$
DECLARE
    rand_val NUMERIC;
BEGIN
    rand_val := RANDOM();

    -- Distribution: 10% have 0, 20% have 1, 30% have 2, 20% have 3, etc.
    CASE
        WHEN rand_val < 0.10 THEN RETURN 0;
        WHEN rand_val < 0.30 THEN RETURN 1;
        WHEN rand_val < 0.60 THEN RETURN 2;
        WHEN rand_val < 0.80 THEN RETURN 3;
        WHEN rand_val < 0.90 THEN RETURN 4;
        WHEN rand_val < 0.95 THEN RETURN 5;
        WHEN rand_val < 0.98 THEN RETURN 6;
        WHEN rand_val < 0.99 THEN RETURN 7;
        ELSE RETURN 8;
    END CASE;
END;
$$ LANGUAGE plpgsql;

-- ========================================
-- START DATA GENERATION
-- ========================================

\echo '========================================'
\echo 'TAX PROCESSING TEST DATA GENERATOR (PostgreSQL)'
\echo '========================================'

-- Clear existing data for this tax year
DELETE FROM tax.transactions WHERE tax_year = :'tax_year';
DELETE FROM tax.position_summary WHERE tax_year = :'tax_year';
DELETE FROM tax.opening_balances WHERE tax_year = :'tax_year';
DELETE FROM tax.processing_status WHERE tax_year = :'tax_year';

\echo 'Cleared existing data for tax year' :'tax_year'

-- ========================================
-- STEP 1: GENERATE OPENING BALANCES
-- ========================================

\echo 'Step 1: Generating opening balances...'

-- Create temporary table for instruments with weights
CREATE TEMP TABLE temp_instruments AS
SELECT * FROM (
    VALUES
        -- Large Cap Tech (High Weight)
        ('AAPL', 15), ('MSFT', 12), ('GOOGL', 10), ('AMZN', 8), ('TSLA', 7),
        ('META', 6), ('NFLX', 5), ('NVDA', 4), ('AMD', 4), ('CRM', 3),

        -- Traditional Large Cap (Medium Weight)
        ('ORCL', 3), ('IBM', 3), ('INTC', 3), ('CSCO', 2), ('ADBE', 2),
        ('JPM', 3), ('BAC', 2), ('WFC', 2), ('C', 2), ('GS', 2),
        ('JNJ', 3), ('PFE', 2), ('MRK', 2), ('ABBV', 2), ('UNH', 3),

        -- Growth/Emerging (Low Weight)
        ('PYPL', 2), ('SQ', 2), ('SHOP', 2), ('SPOT', 1), ('ZM', 1),
        ('ROKU', 1), ('TWTR', 1), ('SNAP', 1), ('UBER', 1), ('LYFT', 1),
        ('DOCU', 1), ('OKTA', 1), ('SNOW', 1), ('PLTR', 1), ('COIN', 1),

        -- ETFs and Bonds (Medium Weight)
        ('SPY', 5), ('QQQ', 4), ('VTI', 3), ('IWM', 2), ('TLT', 2)
) AS instruments(symbol, weight);

-- Generate weighted instrument list for selection
CREATE TEMP TABLE temp_weighted_instruments AS
SELECT symbol
FROM temp_instruments,
     LATERAL generate_series(1, weight) AS gs;

-- Generate opening balances
WITH account_numbers AS (
    SELECT 'ACC' || LPAD((ROW_NUMBER() OVER ())::TEXT, 6, '0') AS account_id
    FROM generate_series(1, :opening_balance_count)
),
random_instruments AS (
    SELECT
        an.account_id,
        (SELECT symbol FROM temp_weighted_instruments ORDER BY RANDOM() LIMIT 1) AS instrument
    FROM account_numbers an
)
INSERT INTO tax.opening_balances (account_id, instrument, tax_year, units_held, book_cost)
SELECT
    ri.account_id,
    ri.instrument,
    :'tax_year',
    ROUND(random_between(:min_units_held, :max_units_held), 2),
    ROUND(
        random_between(:min_units_held, :max_units_held) *
        random_between(:min_price_per_unit, :max_price_per_unit),
        2
    )
FROM random_instruments ri;

-- Get actual count
DO $$
DECLARE
    actual_opening_balances INTEGER;
BEGIN
    SELECT COUNT(*) INTO actual_opening_balances
    FROM tax.opening_balances
    WHERE tax_year = :'tax_year';

    RAISE NOTICE 'Generated % opening balances', actual_opening_balances;
END $$;

-- ========================================
-- STEP 2: GENERATE TRANSACTIONS
-- ========================================

\echo 'Step 2: Generating transactions...'

-- Create transaction types with weights
CREATE TEMP TABLE temp_transaction_types AS
SELECT * FROM (
    VALUES
        ('BUY', 40),
        ('SELL', 35),
        ('DIVIDEND', 20),
        ('SPLIT', 3),
        ('SPINOFF', 2)
) AS types(type_name, weight);

-- Generate weighted transaction types
CREATE TEMP TABLE temp_weighted_transaction_types AS
SELECT type_name
FROM temp_transaction_types,
     LATERAL generate_series(1, weight) AS gs;

-- Generate transactions for each opening balance
WITH position_transaction_counts AS (
    SELECT
        account_id,
        instrument,
        units_held,
        book_cost,
        get_transaction_count() AS transaction_count
    FROM tax.opening_balances
    WHERE tax_year = :'tax_year'
),
transaction_series AS (
    SELECT
        ptc.*,
        generate_series(1, ptc.transaction_count) AS txn_seq
    FROM position_transaction_counts ptc
    WHERE ptc.transaction_count > 0
),
transaction_data AS (
    SELECT
        ts.*,
        'TXN' || :'tax_year' || '_' ||
        LPAD((ROW_NUMBER() OVER (ORDER BY ts.account_id, ts.txn_seq))::TEXT, 10, '0') AS transaction_id,
        (:'tax_year' || '-01-01')::DATE +
        INTERVAL '1 day' * FLOOR(RANDOM() * 365) AS transaction_date,
        (SELECT type_name FROM temp_weighted_transaction_types ORDER BY RANDOM() LIMIT 1) AS transaction_type
    FROM transaction_series ts
)
INSERT INTO tax.transactions (
    transaction_id, account_id, instrument, tax_year, transaction_date,
    transaction_type, units, price_per_unit, total_amount
)
SELECT
    td.transaction_id,
    td.account_id,
    td.instrument,
    :'tax_year',
    td.transaction_date,
    td.transaction_type,
    -- Calculate units based on transaction type
    CASE td.transaction_type
        WHEN 'BUY' THEN ROUND(random_between(:min_units_held, :max_units_held), 2)
        WHEN 'SELL' THEN -ROUND(random_between(1, td.units_held * 0.5), 2)
        WHEN 'DIVIDEND' THEN 0
        WHEN 'SPLIT' THEN td.units_held
        WHEN 'SPINOFF' THEN ROUND(td.units_held * 0.1, 2)
        ELSE 0
    END,
    -- Calculate price per unit
    CASE td.transaction_type
        WHEN 'BUY' THEN ROUND(random_between(:min_price_per_unit, :max_price_per_unit), 2)
        WHEN 'SELL' THEN ROUND(random_between(:min_price_per_unit, :max_price_per_unit), 2)
        WHEN 'DIVIDEND' THEN ROUND(random_between(0.5, 5.0), 2)
        WHEN 'SPLIT' THEN NULL
        WHEN 'SPINOFF' THEN ROUND(random_between(10, 100), 2)
        ELSE NULL
    END,
    -- Calculate total amount
    CASE td.transaction_type
        WHEN 'BUY' THEN
            ROUND(
                random_between(:min_units_held, :max_units_held) *
                random_between(:min_price_per_unit, :max_price_per_unit),
                2
            )
        WHEN 'SELL' THEN
            -ROUND(
                random_between(1, td.units_held * 0.5) *
                random_between(:min_price_per_unit, :max_price_per_unit),
                2
            )
        WHEN 'DIVIDEND' THEN
            ROUND(td.units_held * random_between(0.5, 5.0), 2)
        WHEN 'SPLIT' THEN 0
        WHEN 'SPINOFF' THEN
            ROUND(td.units_held * 0.1 * random_between(10, 100), 2)
        ELSE 0
    END
FROM transaction_data td;

-- ========================================
-- STEP 3: CREATE PROCESSING STATUS
-- ========================================

INSERT INTO tax.processing_status (batch_id, tax_year, status, total_positions)
SELECT
    'batch-' || :'tax_year' || '-001',
    :'tax_year',
    'PENDING',
    COUNT(*)
FROM tax.opening_balances
WHERE tax_year = :'tax_year';

-- ========================================
-- STEP 4: GENERATE SUMMARY STATISTICS
-- ========================================

DO $$
DECLARE
    actual_opening_balances INTEGER;
    total_transactions INTEGER;
    actual_avg_transactions NUMERIC;
BEGIN
    -- Get counts
    SELECT COUNT(*) INTO actual_opening_balances
    FROM tax.opening_balances
    WHERE tax_year = :'tax_year';

    SELECT COUNT(*) INTO total_transactions
    FROM tax.transactions
    WHERE tax_year = :'tax_year';

    actual_avg_transactions := ROUND(total_transactions::NUMERIC / actual_opening_balances, 2);

    -- Display summary
    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'DATA GENERATION COMPLETE';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Tax Year: %', :'tax_year';
    RAISE NOTICE 'Opening Balances: %', actual_opening_balances;
    RAISE NOTICE 'Total Transactions: %', total_transactions;
    RAISE NOTICE 'Avg Transactions per Position: %', actual_avg_transactions;
END $$;

-- Display transaction distribution
\echo ''
\echo 'Transaction Distribution by Position:'
SELECT
    txn_count,
    position_count,
    ROUND(position_count * 100.0 / SUM(position_count) OVER(), 2) || '%' AS percentage
FROM (
    SELECT
        COALESCE(tc.txn_count, 0) as txn_count,
        COUNT(*) as position_count
    FROM tax.opening_balances ob
    LEFT JOIN (
        SELECT account_id, COUNT(*) as txn_count
        FROM tax.transactions
        WHERE tax_year = :'tax_year'
        GROUP BY account_id
    ) tc ON ob.account_id = tc.account_id
    WHERE ob.tax_year = :'tax_year'
    GROUP BY COALESCE(tc.txn_count, 0)
) dist
ORDER BY txn_count;

\echo ''
\echo 'Transaction Type Distribution:'
SELECT
    transaction_type,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) || '%' AS percentage
FROM tax.transactions
WHERE tax_year = :'tax_year'
GROUP BY transaction_type
ORDER BY COUNT(*) DESC;

\echo ''
\echo 'Top 10 Most Popular Instruments:'
SELECT
    instrument,
    COUNT(*) AS positions,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) || '%' AS percentage
FROM tax.opening_balances
WHERE tax_year = :'tax_year'
GROUP BY instrument
ORDER BY COUNT(*) DESC
LIMIT 10;

\echo ''
\echo '========================================'
\echo 'DATABASE CONNECTION INFO'
\echo '========================================'
\echo 'Host: localhost'
\echo 'Port: 5432'
\echo 'Database: TaxProcessing'
\echo 'Username: taxuser'
\echo 'Password: TaxProcessing123!'
\echo ''
\echo 'Connection string:'
\echo 'postgresql://taxuser:TaxProcessing123!@localhost:5432/TaxProcessing'

-- Clean up temporary tables
DROP TABLE temp_instruments;
DROP TABLE temp_weighted_instruments;
DROP TABLE temp_transaction_types;
DROP TABLE temp_weighted_transaction_types;

-- Clean up functions (optional - keep them for future use)
-- DROP FUNCTION random_between(NUMERIC, NUMERIC);
-- DROP FUNCTION random_int_between(INTEGER, INTEGER);
-- DROP FUNCTION get_transaction_count();