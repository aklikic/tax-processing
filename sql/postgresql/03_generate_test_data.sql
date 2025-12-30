-- Simple Performance Test Data Generator
-- Configurable Opening Balances and Transactions

SET search_path TO tax, public;

-- ========================================
-- CONFIGURATION SECTION
-- ========================================

-- Configuration variables - modify these for different test scenarios

-- Development/Test Configuration
\set target_opening_balances 500000
\set target_avg_transactions_per_position 2.8

-- Performance Target Configuration
-- \set target_opening_balances 4400000
-- \set target_avg_transactions_per_position 2.8

-- Calculate derived values
-- Instruments per account (20 instruments, so accounts = target_balances / 20)
-- If we want exactly target_opening_balances, we need: accounts * 20 = target_opening_balances

\echo '========================================'
\echo 'TEST DATA GENERATOR CONFIGURATION'
\echo '========================================'
\echo 'Target Opening Balances:' :target_opening_balances
\echo 'Target Avg Transactions/Position:' :target_avg_transactions_per_position
\echo 'Expected Total Transactions:' :target_opening_balances * :target_avg_transactions_per_position
\echo ''

-- Clear existing 2023 data
DELETE FROM tax.transactions WHERE tax_year = '2023';
DELETE FROM tax.opening_balances WHERE tax_year = '2023';

\echo 'Generating opening balances...'
\echo 'Target opening balances:' :target_opening_balances

-- Generate Opening Balances (accounts × 20 instruments = target_opening_balances)
-- Calculate number of accounts needed: target_opening_balances / 20
INSERT INTO tax.opening_balances (account_id, instrument, units_held, book_cost, tax_year)
SELECT
    'ACC' || LPAD(account_num::text, 6, '0') as account_id,
    instruments.symbol as instrument,
    ROUND((RANDOM() * 2000 + 100)::numeric, 2) as units_held,
    ROUND((RANDOM() * 200000 + 10000)::numeric, 2) as book_cost,
    '2023' as tax_year
FROM
    generate_series(1, :target_opening_balances / 20) as account_num,
    (VALUES
        ('AAPL'), ('GOOGL'), ('MSFT'), ('AMZN'), ('TSLA'),
        ('META'), ('NVDA'), ('NFLX'), ('ORCL'), ('CRM'),
        ('ADBE'), ('INTC'), ('AMD'), ('IBM'), ('CSCO'),
        ('PYPL'), ('QCOM'), ('AVGO'), ('TXN'), ('AMAT')
    ) as instruments(symbol);

\echo 'Generated opening balances. Generating transactions...'
\echo 'Target avg transactions per position:' :target_avg_transactions_per_position

-- Generate transactions with configurable average per position
-- Create realistic transaction sequences that respect position holdings
INSERT INTO tax.transactions (
    transaction_id, account_id, instrument, transaction_date,
    transaction_type, units, price_per_unit, total_amount, tax_year
)
SELECT
    'TXN2023_' || LPAD(ROW_NUMBER() OVER (ORDER BY ob.account_id, ob.instrument, txn_gen.txn_seq)::text, 10, '0') as transaction_id,
    ob.account_id,
    ob.instrument,
    '2023-01-01'::date + (txn_gen.txn_seq * 30 + (RANDOM() * 25)::int) as transaction_date, -- Spread over year
    -- Predefined transaction packages based on sequence number
    CASE
        -- 1 transaction: BUY 100
        WHEN total_transactions = 1 THEN 'BUY'
        -- 2 transactions: BUY 100, SELL 50
        WHEN total_transactions = 2 AND txn_gen.txn_seq = 1 THEN 'BUY'
        WHEN total_transactions = 2 AND txn_gen.txn_seq = 2 THEN 'SELL'
        -- 3 transactions: BUY 200, SELL 50, SELL 50
        WHEN total_transactions = 3 AND txn_gen.txn_seq = 1 THEN 'BUY'
        WHEN total_transactions = 3 AND txn_gen.txn_seq = 2 THEN 'SELL'
        WHEN total_transactions = 3 AND txn_gen.txn_seq = 3 THEN 'SELL'
        -- 4+ transactions: BUY, SELL, BUY, SELL pattern
        WHEN txn_gen.txn_seq = 1 THEN 'BUY'
        WHEN txn_gen.txn_seq % 2 = 0 THEN 'SELL'
        ELSE 'BUY'
    END as transaction_type,
    -- Predefined units for each transaction package
    CASE
        -- 1 transaction: BUY 100
        WHEN total_transactions = 1 THEN 100
        -- 2 transactions: BUY 100, SELL 50
        WHEN total_transactions = 2 AND txn_gen.txn_seq = 1 THEN 100
        WHEN total_transactions = 2 AND txn_gen.txn_seq = 2 THEN 50
        -- 3 transactions: BUY 200, SELL 50, SELL 50
        WHEN total_transactions = 3 AND txn_gen.txn_seq = 1 THEN 200
        WHEN total_transactions = 3 AND txn_gen.txn_seq = 2 THEN 50
        WHEN total_transactions = 3 AND txn_gen.txn_seq = 3 THEN 50
        -- 4+ transactions: larger BUYs, smaller SELLs
        WHEN txn_gen.txn_seq = 1 THEN 300
        WHEN txn_gen.txn_seq % 2 = 0 THEN 75
        ELSE 150
    END as units,
    ROUND((RANDOM() * 300 + 50)::numeric, 6) as price_per_unit,
    ROUND((RANDOM() * 150000 + 1000)::numeric, 2) as total_amount,
    '2023' as tax_year
FROM
    tax.opening_balances ob
    CROSS JOIN LATERAL (
        -- Generate transactions based on target average per position
        -- Use a seeded random based on position ID to ensure different results per position
        WITH position_seed AS (
            SELECT (ABS(HASHTEXT(ob.account_id || '_' || ob.instrument)) % 1000000) / 1000000.0 AS rand_val
        ),
        transaction_count AS (
            SELECT CASE
                -- Adjust distribution based on target_avg_transactions_per_position
                WHEN :target_avg_transactions_per_position <= 1.0 THEN
                    CASE WHEN ps.rand_val < 0.3 THEN 0 WHEN ps.rand_val < 0.8 THEN 1 ELSE 2 END
                WHEN :target_avg_transactions_per_position <= 2.0 THEN
                    CASE WHEN ps.rand_val < 0.70 THEN 1 WHEN ps.rand_val < 0.90 THEN 2 ELSE 3 END
                WHEN :target_avg_transactions_per_position <= 3.0 THEN
                    CASE WHEN ps.rand_val < 0.05 THEN 0 WHEN ps.rand_val < 0.20 THEN 1 WHEN ps.rand_val < 0.45 THEN 2 WHEN ps.rand_val < 0.70 THEN 3 WHEN ps.rand_val < 0.85 THEN 4 ELSE 5 END
                WHEN :target_avg_transactions_per_position <= 5.0 THEN
                    CASE WHEN ps.rand_val < 0.02 THEN 0 WHEN ps.rand_val < 0.10 THEN 1 WHEN ps.rand_val < 0.25 THEN 2 WHEN ps.rand_val < 0.45 THEN 3 WHEN ps.rand_val < 0.65 THEN 4 WHEN ps.rand_val < 0.80 THEN 5 WHEN ps.rand_val < 0.90 THEN 6 WHEN ps.rand_val < 0.96 THEN 7 ELSE 8 END
                ELSE
                    -- For high averages, generate more transactions
                    CASE WHEN ps.rand_val < 0.01 THEN 0 WHEN ps.rand_val < 0.05 THEN 1 WHEN ps.rand_val < 0.15 THEN 2 WHEN ps.rand_val < 0.30 THEN 3 WHEN ps.rand_val < 0.50 THEN 4 WHEN ps.rand_val < 0.70 THEN 5 WHEN ps.rand_val < 0.85 THEN 6 WHEN ps.rand_val < 0.95 THEN 7 ELSE 8 END
            END as total_transactions
            FROM position_seed ps
        )
        SELECT generate_series(1, tc.total_transactions) as txn_seq, tc.total_transactions
        FROM transaction_count tc
    ) txn_gen
WHERE ob.tax_year = '2023'
  AND txn_gen.txn_seq > 0;

\echo 'Data generation complete!'

-- Performance verification
\echo ''
\echo 'Performance Target Verification:'

SELECT
    'Opening Balances' as metric,
    COUNT(*) as actual_count,
    :'target_opening_balances' as target,
    CASE WHEN COUNT(*) >= :target_opening_balances THEN '✅ TARGET MET' ELSE '❌ BELOW TARGET' END as status
FROM tax.opening_balances
WHERE tax_year = '2023'

UNION ALL

SELECT
    'Transactions' as metric,
    COUNT(*) as actual_count,
    ROUND(:target_opening_balances * :target_avg_transactions_per_position) || '' as target,
    CASE WHEN COUNT(*)::numeric >= (:target_opening_balances * :target_avg_transactions_per_position * 0.9)
         THEN '✅ TARGET MET' ELSE '❌ BELOW TARGET' END as status
FROM tax.transactions
WHERE tax_year = '2023'

UNION ALL

SELECT
    'Avg Trans/Position' as metric,
    ROUND((SELECT COUNT(*) FROM tax.transactions WHERE tax_year = '2023')::numeric /
          (SELECT COUNT(*) FROM tax.opening_balances WHERE tax_year = '2023'), 1) as actual_count,
    :'target_avg_transactions_per_position' as target,
    CASE
        WHEN (SELECT COUNT(*) FROM tax.transactions WHERE tax_year = '2023')::numeric /
             (SELECT COUNT(*) FROM tax.opening_balances WHERE tax_year = '2023') >= (:target_avg_transactions_per_position * 0.9)
        THEN '✅ TARGET MET'
        ELSE '❌ BELOW TARGET'
    END as status;

-- Sample data preview
\echo ''
\echo 'Sample Data:'
SELECT 'Opening Balances Sample:' as info, '' as account_id, '' as instrument, '' as units_held
UNION ALL
SELECT '', account_id, instrument, units_held::text
FROM tax.opening_balances
WHERE tax_year = '2023'
LIMIT 3;

SELECT 'Transactions Sample:' as info, '' as account_id, '' as instrument, '' as transaction_type
UNION ALL
SELECT '', account_id, instrument, transaction_type
FROM tax.transactions
WHERE tax_year = '2023'
LIMIT 3;

\echo ''
\echo 'Ready for performance testing!'