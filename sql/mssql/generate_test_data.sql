-- Generate Configurable Test Data for Tax Processing System
-- Usage: Modify the configuration variables below to generate different test scenarios

USE TaxProcessing;
GO

-- ========================================
-- CONFIGURATION SECTION - MODIFY THESE VALUES
-- ========================================

-- Basic Configuration
DECLARE @TaxYear NVARCHAR(4) = '2023';
DECLARE @OpeningBalanceCount INT = 100000; -- Number of opening balances to generate

-- Transaction Configuration per Position
DECLARE @MinTransactionsPerPosition INT = 0; -- Minimum transactions per position
DECLARE @MaxTransactionsPerPosition INT = 8; -- Maximum transactions per position
DECLARE @AvgTransactionsPerPosition DECIMAL(3,1) = 2.8; -- Target average (affects distribution)

-- Data Distribution Weights (adjust percentages)
-- Positions with N transactions (must sum to 100%)
DECLARE @Pct_0_Transactions INT = 10; -- 10% positions have 0 transactions
DECLARE @Pct_1_Transactions INT = 20; -- 20% positions have 1 transaction
DECLARE @Pct_2_Transactions INT = 30; -- 30% positions have 2 transactions
DECLARE @Pct_3_Transactions INT = 20; -- 20% positions have 3 transactions
DECLARE @Pct_4_Transactions INT = 10; -- 10% positions have 4 transactions
DECLARE @Pct_5_Transactions INT = 5;  -- 5% positions have 5 transactions
DECLARE @Pct_6_Transactions INT = 3;  -- 3% positions have 6 transactions
DECLARE @Pct_7_Transactions INT = 1;  -- 1% positions have 7 transactions
DECLARE @Pct_8_Transactions INT = 1;  -- 1% positions have 8 transactions

-- Value Ranges
DECLARE @MinUnitsHeld DECIMAL(18,6) = 10;      -- Minimum units in opening balance
DECLARE @MaxUnitsHeld DECIMAL(18,6) = 10000;   -- Maximum units in opening balance
DECLARE @MinPricePerUnit DECIMAL(18,6) = 5;    -- Minimum price per unit
DECLARE @MaxPricePerUnit DECIMAL(18,6) = 500;  -- Maximum price per unit

-- Batch Processing Configuration
DECLARE @BatchSize INT = 10000; -- Process transactions in batches of this size

-- ========================================
-- INSTRUMENT CONFIGURATION
-- ========================================

-- Popular instruments with realistic weights for random selection
DECLARE @Instruments TABLE (Symbol NVARCHAR(20), Weight INT);
INSERT INTO @Instruments VALUES
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
    ('SPY', 5), ('QQQ', 4), ('VTI', 3), ('IWM', 2), ('TLT', 2);

-- Transaction types with realistic distribution
DECLARE @TransactionTypes TABLE (Type NVARCHAR(20), Weight INT, Description NVARCHAR(100));
INSERT INTO @TransactionTypes VALUES
    ('BUY', 40, 'Purchase of securities'),
    ('SELL', 35, 'Sale of securities'),
    ('DIVIDEND', 20, 'Dividend payment'),
    ('SPLIT', 3, 'Stock split'),
    ('SPINOFF', 2, 'Corporate spinoff');

-- ========================================
-- VALIDATION AND STARTUP
-- ========================================

-- Validate configuration
IF @Pct_0_Transactions + @Pct_1_Transactions + @Pct_2_Transactions + @Pct_3_Transactions +
   @Pct_4_Transactions + @Pct_5_Transactions + @Pct_6_Transactions + @Pct_7_Transactions + @Pct_8_Transactions != 100
BEGIN
    RAISERROR('Transaction distribution percentages must sum to 100%', 16, 1);
    RETURN;
END

PRINT '========================================';
PRINT 'TAX PROCESSING TEST DATA GENERATOR';
PRINT '========================================';
PRINT 'Tax Year: ' + @TaxYear;
PRINT 'Opening Balances: ' + FORMAT(@OpeningBalanceCount, 'N0');
PRINT 'Transaction Range: ' + CAST(@MinTransactionsPerPosition AS NVARCHAR(2)) + ' - ' + CAST(@MaxTransactionsPerPosition AS NVARCHAR(2)) + ' per position';
PRINT 'Target Avg Transactions/Position: ' + CAST(@AvgTransactionsPerPosition AS NVARCHAR(10));
PRINT 'Expected Total Transactions: ' + FORMAT(CAST(@OpeningBalanceCount * @AvgTransactionsPerPosition AS INT), 'N0');
PRINT '';

-- ========================================
-- STEP 1: GENERATE OPENING BALANCES
-- ========================================

PRINT 'Step 1: Generating opening balances...';

-- Clear existing data for this tax year
DELETE FROM tax.Transactions WHERE TaxYear = @TaxYear;
DELETE FROM tax.PositionSummary WHERE TaxYear = @TaxYear;
DELETE FROM tax.OpeningBalances WHERE TaxYear = @TaxYear;
DELETE FROM tax.ProcessingStatus WHERE TaxYear = @TaxYear;

PRINT 'Cleared existing data for tax year ' + @TaxYear;

-- Generate account numbers and assign instruments
WITH AccountNumbers AS (
    SELECT TOP (@OpeningBalanceCount)
        'ACC' + RIGHT('000000' + CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS NVARCHAR(6)), 6) AS AccountId
    FROM sys.objects o1
    CROSS JOIN sys.objects o2
),
WeightedInstruments AS (
    SELECT Symbol
    FROM @Instruments i
    CROSS JOIN (
        -- Repeat each instrument based on its weight for random selection
        SELECT TOP (i.Weight) 1 as x FROM sys.objects
    ) w
),
RandomInstruments AS (
    SELECT
        an.AccountId,
        wi.Symbol,
        ROW_NUMBER() OVER (PARTITION BY an.AccountId ORDER BY NEWID()) AS rn
    FROM AccountNumbers an
    CROSS JOIN WeightedInstruments wi
),
OpeningBalanceData AS (
    SELECT
        ri.AccountId,
        ri.Symbol AS Instrument,
        @TaxYear AS TaxYear,
        -- Random units held within configured range
        ROUND(@MinUnitsHeld + (RAND(CHECKSUM(NEWID())) * (@MaxUnitsHeld - @MinUnitsHeld)), 2) AS UnitsHeld,
        -- Random book cost = units * random price per unit
        ROUND(
            (@MinUnitsHeld + (RAND(CHECKSUM(NEWID())) * (@MaxUnitsHeld - @MinUnitsHeld))) *
            (@MinPricePerUnit + (RAND(CHECKSUM(NEWID())) * (@MaxPricePerUnit - @MinPricePerUnit))),
            2
        ) AS BookCost
    FROM RandomInstruments ri
    WHERE ri.rn = 1 -- One instrument per account
)
INSERT INTO tax.OpeningBalances (AccountId, Instrument, TaxYear, UnitsHeld, BookCost)
SELECT AccountId, Instrument, TaxYear, UnitsHeld, BookCost
FROM OpeningBalanceData;

DECLARE @ActualOpeningBalances INT = @@ROWCOUNT;
PRINT 'Generated ' + FORMAT(@ActualOpeningBalances, 'N0') + ' opening balances';

-- ========================================
-- STEP 2: GENERATE TRANSACTIONS
-- ========================================

PRINT 'Step 2: Generating transactions...';

-- Create temporary table for batch processing
CREATE TABLE #TransactionBatch (
    TransactionId NVARCHAR(50),
    AccountId NVARCHAR(50),
    Instrument NVARCHAR(20),
    TaxYear NVARCHAR(4),
    TransactionDate DATETIME2,
    TransactionType NVARCHAR(20),
    Units DECIMAL(18,6),
    PricePerUnit DECIMAL(18,6),
    TotalAmount DECIMAL(18,2)
);

DECLARE @ProcessedPositions INT = 0;
DECLARE @TotalTransactions INT = 0;
DECLARE @CurrentBatchTransactions INT;

WHILE @ProcessedPositions < @ActualOpeningBalances
BEGIN
    TRUNCATE TABLE #TransactionBatch;

    WITH PositionBatch AS (
        SELECT
            AccountId,
            Instrument,
            UnitsHeld,
            BookCost
        FROM tax.OpeningBalances
        WHERE TaxYear = @TaxYear
        ORDER BY Id
        OFFSET @ProcessedPositions ROWS
        FETCH NEXT @BatchSize ROWS ONLY
    ),
    TransactionCounts AS (
        SELECT
            pb.*,
            -- Distribute transactions based on configured percentages
            CASE
                WHEN RAND(CHECKSUM(NEWID())) < (@Pct_0_Transactions / 100.0) THEN 0
                WHEN RAND(CHECKSUM(NEWID())) < ((@Pct_0_Transactions + @Pct_1_Transactions) / 100.0) THEN 1
                WHEN RAND(CHECKSUM(NEWID())) < ((@Pct_0_Transactions + @Pct_1_Transactions + @Pct_2_Transactions) / 100.0) THEN 2
                WHEN RAND(CHECKSUM(NEWID())) < ((@Pct_0_Transactions + @Pct_1_Transactions + @Pct_2_Transactions + @Pct_3_Transactions) / 100.0) THEN 3
                WHEN RAND(CHECKSUM(NEWID())) < ((@Pct_0_Transactions + @Pct_1_Transactions + @Pct_2_Transactions + @Pct_3_Transactions + @Pct_4_Transactions) / 100.0) THEN 4
                WHEN RAND(CHECKSUM(NEWID())) < ((@Pct_0_Transactions + @Pct_1_Transactions + @Pct_2_Transactions + @Pct_3_Transactions + @Pct_4_Transactions + @Pct_5_Transactions) / 100.0) THEN 5
                WHEN RAND(CHECKSUM(NEWID())) < (90.0 / 100.0) THEN 6 -- 90% cumulative
                WHEN RAND(CHECKSUM(NEWID())) < (99.0 / 100.0) THEN 7 -- 99% cumulative
                ELSE 8
            END AS TransactionCount
        FROM PositionBatch pb
    ),
    NumberSeries AS (
        SELECT TOP (@MaxTransactionsPerPosition) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
        FROM sys.objects
    ),
    TransactionGenerator AS (
        SELECT
            tc.AccountId,
            tc.Instrument,
            tc.UnitsHeld,
            tc.BookCost,
            ns.n AS TransactionSequence,
            -- Random transaction date in the tax year
            DATEADD(DAY,
                FLOOR(RAND(CHECKSUM(NEWID())) * 365),
                CAST(@TaxYear + '-01-01' AS DATETIME2)
            ) AS TransactionDate,
            -- Weighted random transaction type
            (SELECT TOP 1 Type FROM @TransactionTypes tt
             CROSS JOIN (SELECT TOP (tt.Weight) 1 as x FROM sys.objects) w
             ORDER BY NEWID()) AS TransactionType
        FROM TransactionCounts tc
        CROSS JOIN NumberSeries ns
        WHERE ns.n <= tc.TransactionCount
    )
    INSERT INTO #TransactionBatch (
        TransactionId,
        AccountId,
        Instrument,
        TaxYear,
        TransactionDate,
        TransactionType,
        Units,
        PricePerUnit,
        TotalAmount
    )
    SELECT
        'TXN' + @TaxYear + '_' +
        RIGHT('0000000000' + CAST(ROW_NUMBER() OVER (ORDER BY AccountId, TransactionDate) + @TotalTransactions AS NVARCHAR(10)), 10) AS TransactionId,
        AccountId,
        Instrument,
        @TaxYear,
        TransactionDate,
        TransactionType,
        -- Calculate units based on transaction type
        CASE TransactionType
            WHEN 'BUY' THEN ROUND(@MinUnitsHeld + (RAND(CHECKSUM(NEWID())) * (@MaxUnitsHeld - @MinUnitsHeld)), 2)
            WHEN 'SELL' THEN -ROUND(1 + (RAND(CHECKSUM(NEWID())) * (UnitsHeld * 0.5)), 2)  -- Sell up to 50% of holdings
            WHEN 'DIVIDEND' THEN 0  -- No units change for dividends
            WHEN 'SPLIT' THEN UnitsHeld  -- Stock split doubles shares
            WHEN 'SPINOFF' THEN ROUND(UnitsHeld * 0.1, 2)  -- Spinoff 10% of shares
            ELSE 0
        END AS Units,
        -- Price per unit (varies by transaction type)
        CASE TransactionType
            WHEN 'BUY' THEN ROUND(@MinPricePerUnit + (RAND(CHECKSUM(NEWID())) * (@MaxPricePerUnit - @MinPricePerUnit)), 2)
            WHEN 'SELL' THEN ROUND(@MinPricePerUnit + (RAND(CHECKSUM(NEWID())) * (@MaxPricePerUnit - @MinPricePerUnit)), 2)
            WHEN 'DIVIDEND' THEN ROUND(0.5 + (RAND(CHECKSUM(NEWID())) * 4.5), 2)  -- $0.50-5.00 per share dividend
            WHEN 'SPLIT' THEN NULL  -- No price for splits
            WHEN 'SPINOFF' THEN ROUND(10 + (RAND(CHECKSUM(NEWID())) * 90), 2)  -- $10-100 for spinoff
            ELSE NULL
        END AS PricePerUnit,
        -- Calculate total amount
        CASE TransactionType
            WHEN 'BUY' THEN
                ROUND(
                    (@MinUnitsHeld + (RAND(CHECKSUM(NEWID())) * (@MaxUnitsHeld - @MinUnitsHeld))) *
                    (@MinPricePerUnit + (RAND(CHECKSUM(NEWID())) * (@MaxPricePerUnit - @MinPricePerUnit))),
                    2
                )
            WHEN 'SELL' THEN
                -ROUND(
                    (1 + (RAND(CHECKSUM(NEWID())) * (UnitsHeld * 0.5))) *
                    (@MinPricePerUnit + (RAND(CHECKSUM(NEWID())) * (@MaxPricePerUnit - @MinPricePerUnit))),
                    2
                )
            WHEN 'DIVIDEND' THEN
                ROUND(UnitsHeld * (0.5 + (RAND(CHECKSUM(NEWID())) * 4.5)), 2)
            WHEN 'SPLIT' THEN 0
            WHEN 'SPINOFF' THEN
                ROUND((UnitsHeld * 0.1) * (10 + (RAND(CHECKSUM(NEWID())) * 90)), 2)
            ELSE 0
        END AS TotalAmount
    FROM TransactionGenerator;

    -- Insert batch into main table
    INSERT INTO tax.Transactions (
        TransactionId, AccountId, Instrument, TaxYear, TransactionDate,
        TransactionType, Units, PricePerUnit, TotalAmount
    )
    SELECT
        TransactionId, AccountId, Instrument, TaxYear, TransactionDate,
        TransactionType, Units, PricePerUnit, TotalAmount
    FROM #TransactionBatch
    ORDER BY AccountId, Instrument, TransactionDate;

    SET @CurrentBatchTransactions = @@ROWCOUNT;
    SET @TotalTransactions = @TotalTransactions + @CurrentBatchTransactions;
    SET @ProcessedPositions = @ProcessedPositions + @BatchSize;

    PRINT 'Processed ' + FORMAT(LEAST(@ProcessedPositions, @ActualOpeningBalances), 'N0') +
          ' / ' + FORMAT(@ActualOpeningBalances, 'N0') +
          ' positions, generated ' + FORMAT(@TotalTransactions, 'N0') + ' transactions';
END;

DROP TABLE #TransactionBatch;

-- ========================================
-- STEP 3: CREATE PROCESSING STATUS
-- ========================================

INSERT INTO tax.ProcessingStatus (BatchId, TaxYear, Status, TotalPositions)
VALUES ('batch-' + @TaxYear + '-001', @TaxYear, 'PENDING', @ActualOpeningBalances);

-- ========================================
-- STEP 4: GENERATE SUMMARY STATISTICS
-- ========================================

DECLARE @ActualAvgTransactions DECIMAL(5,2) = CAST(@TotalTransactions AS DECIMAL(10,2)) / @ActualOpeningBalances;

PRINT '';
PRINT '========================================';
PRINT 'DATA GENERATION COMPLETE';
PRINT '========================================';
PRINT 'Configuration Summary:';
PRINT '  Tax Year: ' + @TaxYear;
PRINT '  Opening Balances: ' + FORMAT(@ActualOpeningBalances, 'N0');
PRINT '  Total Transactions: ' + FORMAT(@TotalTransactions, 'N0');
PRINT '  Actual Avg Transactions/Position: ' + CAST(@ActualAvgTransactions AS NVARCHAR(10));
PRINT '  Target Avg Transactions/Position: ' + CAST(@AvgTransactionsPerPosition AS NVARCHAR(10));
PRINT '';

-- Transaction distribution analysis
PRINT 'Actual Transaction Distribution:';
WITH TransactionCounts AS (
    SELECT
        AccountId,
        COUNT(*) as TxnCount
    FROM tax.Transactions
    WHERE TaxYear = @TaxYear
    GROUP BY AccountId
),
DistributionStats AS (
    SELECT
        COALESCE(tc.TxnCount, 0) as TransactionCount,
        COUNT(*) as PositionCount
    FROM tax.OpeningBalances ob
    LEFT JOIN TransactionCounts tc ON ob.AccountId = tc.AccountId
    WHERE ob.TaxYear = @TaxYear
    GROUP BY COALESCE(tc.TxnCount, 0)
)
SELECT
    TransactionCount,
    PositionCount,
    FORMAT(PositionCount * 100.0 / @ActualOpeningBalances, 'N2') + '%' as Percentage
FROM DistributionStats
ORDER BY TransactionCount;

PRINT '';
PRINT 'Transaction Type Distribution:';
SELECT
    TransactionType,
    FORMAT(COUNT(*), 'N0') AS Count,
    FORMAT(COUNT(*) * 100.0 / @TotalTransactions, 'N2') + '%' AS Percentage
FROM tax.Transactions
WHERE TaxYear = @TaxYear
GROUP BY TransactionType
ORDER BY COUNT(*) DESC;

PRINT '';
PRINT 'Top 10 Most Popular Instruments:';
SELECT TOP 10
    Instrument,
    FORMAT(COUNT(*), 'N0') AS Positions,
    FORMAT(COUNT(*) * 100.0 / @ActualOpeningBalances, 'N2') + '%' AS Percentage
FROM tax.OpeningBalances
WHERE TaxYear = @TaxYear
GROUP BY Instrument
ORDER BY COUNT(*) DESC;

PRINT '';
PRINT '========================================';
PRINT 'DATABASE CONNECTION INFO';
PRINT '========================================';
PRINT 'Server: localhost,1433';
PRINT 'Database: TaxProcessing';
PRINT 'Username: SA';
PRINT 'Password: TaxProcessing123!';
PRINT '';
PRINT 'Sample connection string:';
PRINT 'Server=localhost,1433;Database=TaxProcessing;User Id=SA;Password=TaxProcessing123!;TrustServerCertificate=True;';

GO