-- Tax Processing Database Schema
-- Optimized for high-performance batch processing with proper indexing

USE TaxProcessing;
GO

-- Opening Balances Table
-- Stores the starting positions for each account-instrument combination for a tax year
CREATE TABLE tax.OpeningBalances (
    Id BIGINT IDENTITY(1,1) PRIMARY KEY,
    AccountId NVARCHAR(50) NOT NULL,
    Instrument NVARCHAR(20) NOT NULL,
    TaxYear NVARCHAR(4) NOT NULL,
    UnitsHeld DECIMAL(18,6) NOT NULL,
    BookCost DECIMAL(18,2) NOT NULL,
    CreatedAt DATETIME2 DEFAULT GETUTCDATE(),

    -- Composite index for efficient windowed queries
    INDEX IX_OpeningBalances_TaxYear_Id CLUSTERED (TaxYear ASC, Id ASC),

    -- Unique constraint to prevent duplicate positions
    CONSTRAINT UQ_OpeningBalances_Account_Instrument_TaxYear
        UNIQUE (AccountId, Instrument, TaxYear),

    -- Non-clustered index for position lookups
    INDEX IX_OpeningBalances_Position NONCLUSTERED (AccountId, Instrument)
);
GO

-- Transactions Table
-- Stores all transactions that affect positions (buys, sells, dividends, etc.)
-- Partitioned by TaxYear for performance at scale
CREATE TABLE tax.Transactions (
    Id BIGINT IDENTITY(1,1) NOT NULL,
    TransactionId NVARCHAR(50) NOT NULL, -- External transaction ID for idempotency
    AccountId NVARCHAR(50) NOT NULL,
    Instrument NVARCHAR(20) NOT NULL,
    TaxYear NVARCHAR(4) NOT NULL,
    TransactionDate DATETIME2 NOT NULL,
    TransactionType NVARCHAR(20) NOT NULL, -- BUY, SELL, DIVIDEND, SPLIT, etc.
    Units DECIMAL(18,6) NOT NULL, -- Positive for buys, negative for sells
    PricePerUnit DECIMAL(18,6) NULL, -- Price per unit (null for splits/dividends)
    TotalAmount DECIMAL(18,2) NOT NULL, -- Total transaction amount
    CreatedAt DATETIME2 DEFAULT GETUTCDATE(),

    -- Primary key includes tax year for partition elimination
    CONSTRAINT PK_Transactions PRIMARY KEY (Id, TaxYear),

    -- Clustered index optimized for position-based queries with chronological processing
    INDEX IX_Transactions_Position_Date CLUSTERED (AccountId, Instrument, TaxYear, TransactionDate ASC, Id ASC),

    -- Unique constraint on external transaction ID within tax year
    CONSTRAINT UQ_Transactions_TransactionId_TaxYear
        UNIQUE (TransactionId, TaxYear),

    -- Non-clustered index for efficient windowed loading by position batch
    INDEX IX_Transactions_TaxYear_Position NONCLUSTERED (TaxYear, AccountId, Instrument, TransactionDate),

    -- Index for transaction ID lookups (idempotency checks)
    INDEX IX_Transactions_TransactionId NONCLUSTERED (TransactionId)
)
-- Partition by TaxYear for large-scale performance (commented out for simplicity)
-- ON TaxYearPartitionScheme(TaxYear)
GO

-- Position Summary Table (Optional - for reporting/validation)
-- Maintains current position state after processing
CREATE TABLE tax.PositionSummary (
    Id BIGINT IDENTITY(1,1) PRIMARY KEY,
    AccountId NVARCHAR(50) NOT NULL,
    Instrument NVARCHAR(20) NOT NULL,
    TaxYear NVARCHAR(4) NOT NULL,
    UnitsHeld DECIMAL(18,6) NOT NULL,
    BookCost DECIMAL(18,2) NOT NULL,
    RealizedGainLoss DECIMAL(18,2) DEFAULT 0,
    LastProcessedTransactionId BIGINT NULL,
    LastUpdated DATETIME2 DEFAULT GETUTCDATE(),

    -- Clustered index for efficient position lookups
    CONSTRAINT UQ_PositionSummary_Position_TaxYear
        UNIQUE CLUSTERED (AccountId, Instrument, TaxYear),

    -- Non-clustered index for tax year queries
    INDEX IX_PositionSummary_TaxYear NONCLUSTERED (TaxYear),

    -- Foreign key to last processed transaction
    CONSTRAINT FK_PositionSummary_LastTransaction
        FOREIGN KEY (LastProcessedTransactionId, TaxYear)
        REFERENCES tax.Transactions(Id, TaxYear)
);
GO

-- Processing Status Table
-- Tracks batch processing status and progress
CREATE TABLE tax.ProcessingStatus (
    BatchId NVARCHAR(100) PRIMARY KEY,
    TaxYear NVARCHAR(4) NOT NULL,
    Status NVARCHAR(20) NOT NULL, -- PENDING, INITIALIZING, PROCESSING, COMPLETED, FAILED
    TotalPositions BIGINT NULL,
    ProcessedPositions BIGINT DEFAULT 0,
    TotalWindows INT NULL,
    ProcessedWindows INT DEFAULT 0,
    StartTime DATETIME2 DEFAULT GETUTCDATE(),
    EndTime DATETIME2 NULL,
    ErrorMessage NVARCHAR(MAX) NULL,

    INDEX IX_ProcessingStatus_TaxYear NONCLUSTERED (TaxYear),
    INDEX IX_ProcessingStatus_Status NONCLUSTERED (Status)
);
GO

-- Create views for common queries

-- View for positions with their opening balances
CREATE VIEW tax.vw_PositionsWithOpeningBalances AS
SELECT
    ob.AccountId,
    ob.Instrument,
    ob.TaxYear,
    ob.UnitsHeld as OpeningUnits,
    ob.BookCost as OpeningBookCost,
    COALESCE(ps.UnitsHeld, ob.UnitsHeld) as CurrentUnits,
    COALESCE(ps.BookCost, ob.BookCost) as CurrentBookCost,
    COALESCE(ps.RealizedGainLoss, 0) as RealizedGainLoss,
    ps.LastUpdated
FROM tax.OpeningBalances ob
LEFT JOIN tax.PositionSummary ps
    ON ob.AccountId = ps.AccountId
    AND ob.Instrument = ps.Instrument
    AND ob.TaxYear = ps.TaxYear;
GO

-- View for transaction counts by position (useful for debugging)
CREATE VIEW tax.vw_TransactionCountsByPosition AS
SELECT
    AccountId,
    Instrument,
    TaxYear,
    COUNT(*) as TransactionCount,
    MIN(TransactionDate) as FirstTransaction,
    MAX(TransactionDate) as LastTransaction,
    SUM(CASE WHEN TransactionType = 'BUY' THEN Units ELSE 0 END) as TotalBuyUnits,
    SUM(CASE WHEN TransactionType = 'SELL' THEN ABS(Units) ELSE 0 END) as TotalSellUnits
FROM tax.Transactions
GROUP BY AccountId, Instrument, TaxYear;
GO

-- Create stored procedures for batch operations

-- Procedure to get opening balances for a specific window
CREATE PROCEDURE tax.GetOpeningBalancesWindow
    @TaxYear NVARCHAR(4),
    @Offset INT,
    @BatchSize INT
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        AccountId,
        Instrument,
        UnitsHeld,
        BookCost
    FROM tax.OpeningBalances
    WHERE TaxYear = @TaxYear
    ORDER BY Id
    OFFSET @Offset ROWS
    FETCH NEXT @BatchSize ROWS ONLY;
END
GO

-- Procedure to get transactions for a batch of positions
CREATE PROCEDURE tax.GetTransactionsForPositions
    @TaxYear NVARCHAR(4),
    @Positions tax.PositionTableType READONLY, -- Custom table type (defined below)
    @Offset INT,
    @BatchSize INT
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        t.TransactionId,
        t.AccountId,
        t.Instrument,
        t.TransactionDate,
        t.TransactionType,
        t.Units,
        t.PricePerUnit,
        t.TotalAmount
    FROM tax.Transactions t
    INNER JOIN @Positions p ON t.AccountId = p.AccountId AND t.Instrument = p.Instrument
    WHERE t.TaxYear = @TaxYear
    ORDER BY t.AccountId, t.Instrument, t.TransactionDate, t.Id
    OFFSET @Offset ROWS
    FETCH NEXT @BatchSize ROWS ONLY;
END
GO

-- Create custom table type for position parameters
CREATE TYPE tax.PositionTableType AS TABLE (
    AccountId NVARCHAR(50),
    Instrument NVARCHAR(20)
);
GO

-- Procedure to count total opening balances for a tax year
CREATE PROCEDURE tax.CountOpeningBalances
    @TaxYear NVARCHAR(4)
AS
BEGIN
    SET NOCOUNT ON;

    SELECT COUNT(*) as TotalCount
    FROM tax.OpeningBalances
    WHERE TaxYear = @TaxYear;
END
GO

PRINT 'Tax Processing database schema created successfully.';
PRINT 'Tables: OpeningBalances, Transactions, PositionSummary, ProcessingStatus';
PRINT 'Views: vw_PositionsWithOpeningBalances, vw_TransactionCountsByPosition';
PRINT 'Stored Procedures: GetOpeningBalancesWindow, GetTransactionsForPositions, CountOpeningBalances';
PRINT 'Custom Types: PositionTableType';