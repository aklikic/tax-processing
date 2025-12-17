-- Quick Test Scenarios for Tax Processing System
-- Pre-configured scenarios for common testing needs

-- ========================================
-- SCENARIO 1: SMALL SCALE (Development Testing)
-- ========================================
/*
-- 1,000 opening balances, ~2,800 transactions
-- Good for: Development, debugging, quick iteration

USE TaxProcessing;

DECLARE @TaxYear NVARCHAR(4) = '2023';
DECLARE @OpeningBalanceCount INT = 1000;
DECLARE @Pct_0_Transactions INT = 5;   -- 5% no transactions
DECLARE @Pct_1_Transactions INT = 15;  -- 15% one transaction
DECLARE @Pct_2_Transactions INT = 30;  -- 30% two transactions
DECLARE @Pct_3_Transactions INT = 25;  -- 25% three transactions
DECLARE @Pct_4_Transactions INT = 15;  -- 15% four transactions
DECLARE @Pct_5_Transactions INT = 5;   -- 5% five transactions
DECLARE @Pct_6_Transactions INT = 3;   -- 3% six transactions
DECLARE @Pct_7_Transactions INT = 1;   -- 1% seven transactions
DECLARE @Pct_8_Transactions INT = 1;   -- 1% eight transactions

-- Execute the main generate_test_data.sql script with these values
*/

-- ========================================
-- SCENARIO 2: MEDIUM SCALE (Integration Testing)
-- ========================================
/*
-- 50,000 opening balances, ~140,000 transactions
-- Good for: Performance testing, workflow validation

USE TaxProcessing;

DECLARE @TaxYear NVARCHAR(4) = '2023';
DECLARE @OpeningBalanceCount INT = 50000;
DECLARE @Pct_0_Transactions INT = 10;
DECLARE @Pct_1_Transactions INT = 20;
DECLARE @Pct_2_Transactions INT = 25;
DECLARE @Pct_3_Transactions INT = 20;
DECLARE @Pct_4_Transactions INT = 15;
DECLARE @Pct_5_Transactions INT = 5;
DECLARE @Pct_6_Transactions INT = 3;
DECLARE @Pct_7_Transactions INT = 1;
DECLARE @Pct_8_Transactions INT = 1;
*/

-- ========================================
-- SCENARIO 3: LARGE SCALE (Production Simulation)
-- ========================================
/*
-- 1,000,000 opening balances, ~2,800,000 transactions
-- Good for: Load testing, production preparation

USE TaxProcessing;

DECLARE @TaxYear NVARCHAR(4) = '2023';
DECLARE @OpeningBalanceCount INT = 1000000;
DECLARE @Pct_0_Transactions INT = 10;
DECLARE @Pct_1_Transactions INT = 20;
DECLARE @Pct_2_Transactions INT = 30;
DECLARE @Pct_3_Transactions INT = 20;
DECLARE @Pct_4_Transactions INT = 10;
DECLARE @Pct_5_Transactions INT = 5;
DECLARE @Pct_6_Transactions INT = 3;
DECLARE @Pct_7_Transactions INT = 1;
DECLARE @Pct_8_Transactions INT = 1;
*/

-- ========================================
-- SCENARIO 4: FULL SCALE (Production Target)
-- ========================================
/*
-- 4,400,000 opening balances, ~12,300,000 transactions
-- Good for: Production scale testing, performance validation

USE TaxProcessing;

DECLARE @TaxYear NVARCHAR(4) = '2023';
DECLARE @OpeningBalanceCount INT = 4400000;
DECLARE @Pct_0_Transactions INT = 10;
DECLARE @Pct_1_Transactions INT = 20;
DECLARE @Pct_2_Transactions INT = 30;
DECLARE @Pct_3_Transactions INT = 20;
DECLARE @Pct_4_Transactions INT = 10;
DECLARE @Pct_5_Transactions INT = 5;
DECLARE @Pct_6_Transactions INT = 3;
DECLARE @Pct_7_Transactions INT = 1;
DECLARE @Pct_8_Transactions INT = 1;
*/

-- ========================================
-- SCENARIO 5: HIGH TRANSACTION DENSITY
-- ========================================
/*
-- 100,000 opening balances, ~800,000 transactions (8 avg per position)
-- Good for: Testing heavy transaction processing

USE TaxProcessing;

DECLARE @TaxYear NVARCHAR(4) = '2023';
DECLARE @OpeningBalanceCount INT = 100000;
DECLARE @Pct_0_Transactions INT = 0;   -- No positions without transactions
DECLARE @Pct_1_Transactions INT = 0;
DECLARE @Pct_2_Transactions INT = 0;
DECLARE @Pct_3_Transactions INT = 5;
DECLARE @Pct_4_Transactions INT = 10;
DECLARE @Pct_5_Transactions INT = 15;
DECLARE @Pct_6_Transactions INT = 20;
DECLARE @Pct_7_Transactions INT = 25;
DECLARE @Pct_8_Transactions INT = 25;
*/

-- ========================================
-- SCENARIO 6: LOW TRANSACTION DENSITY
-- ========================================
/*
-- 100,000 opening balances, ~50,000 transactions (0.5 avg per position)
-- Good for: Testing sparse transaction scenarios

USE TaxProcessing;

DECLARE @TaxYear NVARCHAR(4) = '2023';
DECLARE @OpeningBalanceCount INT = 100000;
DECLARE @Pct_0_Transactions INT = 50;  -- Half positions have no transactions
DECLARE @Pct_1_Transactions INT = 40;  -- 40% have one transaction
DECLARE @Pct_2_Transactions INT = 10;  -- 10% have two transactions
DECLARE @Pct_3_Transactions INT = 0;
DECLARE @Pct_4_Transactions INT = 0;
DECLARE @Pct_5_Transactions INT = 0;
DECLARE @Pct_6_Transactions INT = 0;
DECLARE @Pct_7_Transactions INT = 0;
DECLARE @Pct_8_Transactions INT = 0;
*/

-- ========================================
-- SCENARIO 7: MULTI-YEAR TESTING
-- ========================================
/*
-- Generate data for multiple tax years
-- Good for: Testing year-over-year processing, data isolation

-- Year 2021: Small dataset
DECLARE @TaxYear NVARCHAR(4) = '2021';
DECLARE @OpeningBalanceCount INT = 10000;
-- [Set distribution percentages...]

-- Year 2022: Medium dataset
DECLARE @TaxYear NVARCHAR(4) = '2022';
DECLARE @OpeningBalanceCount INT = 50000;
-- [Set distribution percentages...]

-- Year 2023: Large dataset
DECLARE @TaxYear NVARCHAR(4) = '2023';
DECLARE @OpeningBalanceCount INT = 100000;
-- [Set distribution percentages...]
*/

-- ========================================
-- UTILITY QUERIES FOR TESTING
-- ========================================

-- Clear all data for a specific tax year
/*
USE TaxProcessing;
DECLARE @TaxYear NVARCHAR(4) = '2023';

DELETE FROM tax.Transactions WHERE TaxYear = @TaxYear;
DELETE FROM tax.PositionSummary WHERE TaxYear = @TaxYear;
DELETE FROM tax.OpeningBalances WHERE TaxYear = @TaxYear;
DELETE FROM tax.ProcessingStatus WHERE TaxYear = @TaxYear;

PRINT 'Cleared all data for tax year ' + @TaxYear;
*/

-- Quick data summary for any tax year
/*
USE TaxProcessing;
DECLARE @TaxYear NVARCHAR(4) = '2023';

SELECT
    'Opening Balances' as DataType,
    FORMAT(COUNT(*), 'N0') as Count
FROM tax.OpeningBalances
WHERE TaxYear = @TaxYear

UNION ALL

SELECT
    'Transactions' as DataType,
    FORMAT(COUNT(*), 'N0') as Count
FROM tax.Transactions
WHERE TaxYear = @TaxYear

UNION ALL

SELECT
    'Unique Positions' as DataType,
    FORMAT(COUNT(DISTINCT AccountId + '|' + Instrument), 'N0') as Count
FROM tax.OpeningBalances
WHERE TaxYear = @TaxYear;
*/

-- Performance test query - simulate batch loading
/*
USE TaxProcessing;
DECLARE @TaxYear NVARCHAR(4) = '2023';
DECLARE @BatchSize INT = 5000;
DECLARE @Offset INT = 0;

-- Test opening balance window loading
SELECT TOP(@BatchSize)
    AccountId,
    Instrument,
    UnitsHeld,
    BookCost
FROM tax.OpeningBalances
WHERE TaxYear = @TaxYear
ORDER BY Id
OFFSET @Offset ROWS;

-- Test transaction loading for positions
DECLARE @Positions tax.PositionTableType;
INSERT INTO @Positions
SELECT DISTINCT AccountId, Instrument
FROM tax.OpeningBalances
WHERE TaxYear = @TaxYear
ORDER BY AccountId
OFFSET @Offset ROWS
FETCH NEXT 111 ROWS ONLY;

EXEC tax.GetTransactionsForPositions @TaxYear, @Positions, 0, 320;
*/