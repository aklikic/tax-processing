# Database Setup for Tax Processing System

This guide explains how to set up and populate the SQL Server database for testing the tax processing system.

## Database Options

### Option 1: PostgreSQL (Recommended - Native ARM64 Support)
```bash
cd sql
docker-compose -f docker-compose-postgresql.yml up -d
```

**✅ Recommended for M1/M2 Macs** - Native ARM64 support, no emulation required.

**Connection Details:**
- **Host:** localhost
- **Port:** 5432
- **Database:** TaxProcessing
- **Username:** taxuser
- **Password:** TaxProcessing123!
- **Web Interface:** http://localhost:8080 (Adminer)

### Option 2: SQL Server (Intel/AMD64 - Emulation Required)
```bash
cd sql
docker-compose -f docker-compose-sqlserver.yml up -d
```

**Connection Details:**
- **Host:** localhost
- **Port:** 1433
- **Database:** TaxProcessing
- **Username:** SA
- **Password:** TaxProcessing123!

### Option 3: Azure SQL Edge (ARM64 - May Have Compatibility Issues)
```bash
cd sql
docker-compose -f docker-compose-sqlserver-arm64.yml up -d
```

**Note:** Azure SQL Edge sometimes has stability issues on certain ARM64 environments.

## Quick Start

1. **Start the database** (PostgreSQL recommended):
```bash
cd sql
docker-compose -f docker-compose-postgresql.yml up -d
```

2. **Wait for database to be ready** (check health status):
```bash
# For PostgreSQL:
cd sql && docker-compose -f docker-compose-postgresql.yml ps

# For SQL Server:
cd sql && docker-compose -f docker-compose-sqlserver.yml ps

# For Azure SQL Edge:
cd sql && docker-compose -f docker-compose-sqlserver-arm64.yml ps
```

3. **Connect to database**:

**PostgreSQL:**
```bash
# Interactive psql session (recommended):
docker exec -it tax-processing-postgresql psql -U taxuser -d TaxProcessing

# Once connected, useful commands:
# \dn                                          # List all schemas
# \dt tax.*                                    # List tables in tax schema
# \d tax.opening_balances                      # Describe table structure
# SELECT COUNT(*) FROM tax.opening_balances;   # Check data counts
# SELECT * FROM tax.opening_balances LIMIT 5;  # Sample data
# \q                                           # Exit

# Single query execution:
docker exec -it tax-processing-postgresql psql -U taxuser -d TaxProcessing -c "SELECT COUNT(*) FROM tax.opening_balances;"

# Schema exploration:
docker exec -it tax-processing-postgresql psql -U taxuser -d TaxProcessing -c "\dt+ tax.*"

# Via web interface: http://localhost:8080
# Server: postgresql, Username: taxuser, Password: TaxProcessing123!, Database: TaxProcessing
```

**SQL Server:**
```bash
# Command line:
docker exec -it tax-processing-mssql /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P "TaxProcessing123!"

# Connection details: localhost,1433 / SA / TaxProcessing123!
```

4. **Run schema and data generation**:

**PostgreSQL:**
```bash
# Create schema first:
docker exec -i tax-processing-postgresql psql -U taxuser -d TaxProcessing < sql/postgresql/01_create_database.sql
docker exec -i tax-processing-postgresql psql -U taxuser -d TaxProcessing < sql/postgresql/02_create_schema.sql

# Generate test data:
docker exec -i tax-processing-postgresql psql -U taxuser -d TaxProcessing < sql/postgresql/generate_test_data.sql
```

**SQL Server:**
```sql
-- Edit the configuration section in sql/generate_test_data.sql first!
:r /sql/generate_test_data.sql
```

## Database Schema

The system creates these main tables:

### `tax.OpeningBalances`
- Stores starting positions for each account-instrument combination
- Clustered index on `(TaxYear, Id)` for efficient windowed queries
- Unique constraint on `(AccountId, Instrument, TaxYear)`

### `tax.Transactions`
- Stores all transactions affecting positions
- Clustered index on `(AccountId, Instrument, TaxYear, TransactionDate, Id)` for chronological processing
- Optimized for loading transactions by position batch

### `tax.PositionSummary` (Optional)
- Maintains current position state after processing
- Used for validation and reporting

### `tax.ProcessingStatus`
- Tracks batch processing progress
- Used by the workflow for monitoring

## Test Data Generation

### Configuration Options

Edit the configuration section in `sql/generate_test_data.sql`:

```sql
-- Basic Configuration
DECLARE @TaxYear NVARCHAR(4) = '2023';
DECLARE @OpeningBalanceCount INT = 100000; -- Number of positions

-- Transaction Distribution (must sum to 100%)
DECLARE @Pct_0_Transactions INT = 10; -- 10% positions have 0 transactions
DECLARE @Pct_1_Transactions INT = 20; -- 20% positions have 1 transaction
DECLARE @Pct_2_Transactions INT = 30; -- 30% positions have 2 transactions
-- ... etc

-- Value Ranges
DECLARE @MinUnitsHeld DECIMAL(18,6) = 10;      -- Min units in opening balance
DECLARE @MaxUnitsHeld DECIMAL(18,6) = 10000;   -- Max units in opening balance
DECLARE @MinPricePerUnit DECIMAL(18,6) = 5;    -- Min price per unit
DECLARE @MaxPricePerUnit DECIMAL(18,6) = 500;  -- Max price per unit
```

### Pre-configured Scenarios

Use `sql/quick_scenarios.sql` for common test scenarios:

| Scenario | Opening Balances | Avg Transactions | Total Transactions | Use Case |
|----------|------------------|------------------|--------------------|----------|
| Small Scale | 1,000 | 2.8 | ~2,800 | Development/Debug |
| Medium Scale | 50,000 | 2.8 | ~140,000 | Integration Testing |
| Large Scale | 1,000,000 | 2.8 | ~2,800,000 | Load Testing |
| Full Scale | 4,400,000 | 2.8 | ~12,300,000 | Production Simulation |
| High Density | 100,000 | 8.0 | ~800,000 | Heavy Transaction Load |
| Low Density | 100,000 | 0.5 | ~50,000 | Sparse Transactions |

### Example: Generate Small Scale Data

```sql
USE TaxProcessing;

-- Edit these values in generate_test_data.sql
DECLARE @OpeningBalanceCount INT = 1000;
DECLARE @Pct_0_Transactions INT = 5;
DECLARE @Pct_1_Transactions INT = 15;
DECLARE @Pct_2_Transactions INT = 30;
DECLARE @Pct_3_Transactions INT = 25;
DECLARE @Pct_4_Transactions INT = 15;
DECLARE @Pct_5_Transactions INT = 5;
DECLARE @Pct_6_Transactions INT = 3;
DECLARE @Pct_7_Transactions INT = 1;
DECLARE @Pct_8_Transactions INT = 1;

-- Then execute: :r /sql/generate_test_data.sql
```

## Database Performance

### Indexing Strategy

The schema is optimized for the workflow's access patterns:

1. **Opening Balance Windows**: `IX_OpeningBalances_TaxYear_Id` enables efficient `OFFSET/FETCH` queries
2. **Transaction Processing**: `IX_Transactions_Position_Date` enables chronological processing per position
3. **Batch Loading**: `IX_Transactions_TaxYear_Position` enables efficient windowed transaction loading

### Connection Pool Configuration

The system is designed for 50 database connections:
- 45 parallel sub-workflows (1 connection each)
- 5 connections reserved for main workflow operations

Configure your connection pool accordingly:
```properties
# Application configuration
database.connection.pool.size=50
database.connection.pool.timeout=30s
```

## Monitoring Queries

### Data Summary
```sql
SELECT
    TaxYear,
    FORMAT(COUNT(*), 'N0') as OpeningBalances,
    FORMAT((SELECT COUNT(*) FROM tax.Transactions t WHERE t.TaxYear = ob.TaxYear), 'N0') as Transactions,
    FORMAT((SELECT COUNT(*) FROM tax.Transactions t WHERE t.TaxYear = ob.TaxYear) * 1.0 / COUNT(*), 'N1') as AvgTxnPerPosition
FROM tax.OpeningBalances ob
GROUP BY TaxYear
ORDER BY TaxYear;
```

### Processing Progress
```sql
SELECT
    BatchId,
    TaxYear,
    Status,
    FORMAT(ProcessedPositions, 'N0') + ' / ' + FORMAT(TotalPositions, 'N0') as Progress,
    CASE
        WHEN TotalPositions > 0
        THEN FORMAT(ProcessedPositions * 100.0 / TotalPositions, 'N1') + '%'
        ELSE 'N/A'
    END as ProgressPct,
    StartTime,
    EndTime
FROM tax.ProcessingStatus
ORDER BY StartTime DESC;
```

### Transaction Distribution
```sql
SELECT
    TxnCount,
    FORMAT(PositionCount, 'N0') as Positions,
    FORMAT(PositionCount * 100.0 / SUM(PositionCount) OVER(), 'N1') + '%' as Percentage
FROM (
    SELECT
        COALESCE(tc.TxnCount, 0) as TxnCount,
        COUNT(*) as PositionCount
    FROM tax.OpeningBalances ob
    LEFT JOIN (
        SELECT AccountId, COUNT(*) as TxnCount
        FROM tax.Transactions
        WHERE TaxYear = '2023'
        GROUP BY AccountId
    ) tc ON ob.AccountId = tc.AccountId
    WHERE ob.TaxYear = '2023'
    GROUP BY COALESCE(tc.TxnCount, 0)
) dist
ORDER BY TxnCount;
```

## Cleanup

### Clear Specific Tax Year
```sql
DECLARE @TaxYear NVARCHAR(4) = '2023';

DELETE FROM tax.Transactions WHERE TaxYear = @TaxYear;
DELETE FROM tax.PositionSummary WHERE TaxYear = @TaxYear;
DELETE FROM tax.OpeningBalances WHERE TaxYear = @TaxYear;
DELETE FROM tax.ProcessingStatus WHERE TaxYear = @TaxYear;
```

### Stop Database
```bash
# For regular SQL Server:
docker-compose down

# For Azure SQL Edge:
docker-compose -f docker-compose-sqlserver-arm64.yml down
```

### Remove All Data
```bash
# For regular SQL Server:
docker-compose down -v  # Removes volumes too

# For Azure SQL Edge:
docker-compose -f docker-compose-sqlserver-arm64.yml down -v
```

## Troubleshooting

### Connection Issues
```bash
# Check if container is running
# For regular SQL Server:
docker-compose ps

# For Azure SQL Edge:
docker-compose -f docker-compose-sqlserver-arm64.yml ps

# Check logs
# For regular SQL Server:
docker-compose logs mssql

# For Azure SQL Edge:
docker-compose -f docker-compose-sqlserver-arm64.yml logs sqlserver-edge

# Test connection
# For regular SQL Server:
docker exec -it tax-processing-mssql /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P "TaxProcessing123!" -Q "SELECT @@VERSION"

# For Azure SQL Edge:
docker exec -it tax-processing-sqlserver-edge /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P "TaxProcessing123!" -Q "SELECT @@VERSION"
```

### Platform Issues (Apple Silicon)

If you get "platform mismatch" errors on M1/M2 Macs:

1. **Use Azure SQL Edge** (recommended):
```bash
docker-compose -f docker-compose-sqlserver-arm64.yml up -d
```

2. **Or force AMD64 emulation**:
```bash
# The regular docker-compose.yml includes platform: linux/amd64
docker-compose up -d
```

**Note:** Azure SQL Edge provides native ARM64 support and identical SQL Server compatibility for better performance on Apple Silicon.

### Performance Issues
```sql
-- Check if indexes are being used
SET STATISTICS IO ON;

-- Test window query performance
SELECT TOP 5000 AccountId, Instrument, UnitsHeld, BookCost
FROM tax.OpeningBalances
WHERE TaxYear = '2023'
ORDER BY Id;

-- Check index usage
SELECT
    i.name as IndexName,
    s.user_seeks,
    s.user_scans,
    s.user_lookups,
    s.user_updates
FROM sys.dm_db_index_usage_stats s
JOIN sys.indexes i ON s.object_id = i.object_id AND s.index_id = i.index_id
WHERE OBJECT_NAME(s.object_id) LIKE '%OpeningBalances%'
ORDER BY s.user_seeks + s.user_scans + s.user_lookups DESC;
```