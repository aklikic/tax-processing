-- Debug script to test transaction generation
-- Run this after generate_test_data.sql to debug transaction issues

SET search_path TO tax, public;

-- Test 1: Check if functions exist
\echo 'Testing function availability:'
SELECT get_transaction_count() as sample_txn_count;

-- Test 2: Check opening balances
\echo 'Opening balance count:'
SELECT COUNT(*) as opening_balance_count FROM tax.opening_balances WHERE tax_year = '2023';

-- Test 3: Test transaction count distribution
\echo 'Testing transaction count distribution:'
WITH test_counts AS (
    SELECT
        account_id,
        get_transaction_count() AS txn_count
    FROM tax.opening_balances
    WHERE tax_year = '2023'
    LIMIT 1000
)
SELECT
    txn_count,
    COUNT(*) as positions_with_this_count
FROM test_counts
GROUP BY txn_count
ORDER BY txn_count;

-- Test 4: Try simple transaction insert for one position
\echo 'Testing simple transaction insert:'
WITH sample_position AS (
    SELECT account_id, instrument, units_held
    FROM tax.opening_balances
    WHERE tax_year = '2023'
    LIMIT 1
)
INSERT INTO tax.transactions (
    transaction_id, account_id, instrument, tax_year,
    transaction_date, transaction_type, units, price_per_unit, total_amount
)
SELECT
    'TEST_TXN_001',
    account_id,
    instrument,
    '2023',
    '2023-06-15'::TIMESTAMPTZ,
    'BUY',
    100.00,
    50.00,
    5000.00
FROM sample_position;

-- Test 5: Check if the insert worked
\echo 'Transaction count after test insert:'
SELECT COUNT(*) as transaction_count FROM tax.transactions WHERE tax_year = '2023';

-- Test 6: Check the test transaction
\echo 'Test transaction details:'
SELECT * FROM tax.transactions WHERE transaction_id = 'TEST_TXN_001';