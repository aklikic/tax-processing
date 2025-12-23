package com.example.infrastructure;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

/**
 * Helper class for setting up test data in PostgreSQL for integration tests.
 * Provides methods to create tables, insert test data, and clean up.
 */
public class PostgreSQLTestDataHelper {

    private final String jdbcUrl;
    private final String username;
    private final String password;

    public PostgreSQLTestDataHelper(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    /**
     * Create the necessary database tables for tax processing.
     */
    public void createTables() throws SQLException {
        try (var connection = DriverManager.getConnection(jdbcUrl, username, password);
             var statement = connection.createStatement()) {

            connection.setAutoCommit(false);

            // Create tax schema if it doesn't exist
            statement.executeUpdate("CREATE SCHEMA IF NOT EXISTS tax");

            statement.executeUpdate("CREATE SEQUENCE IF NOT EXISTS tax.opening_balances_id_seq");

            statement.executeUpdate("CREATE SEQUENCE IF NOT EXISTS tax.transactions_id_seq");

            statement.executeUpdate("""
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
                 )
            """);

            statement.executeUpdate("""
               CREATE INDEX IF NOT EXISTS ix_opening_balances_tax_year_id
                   ON tax.opening_balances (tax_year, id);
            """);

            statement.executeUpdate("""
               CREATE INDEX IF NOT EXISTS ix_opening_balances_position
                   ON tax.opening_balances (account_id, instrument);
            """);

            statement.executeUpdate("""
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
                 )
            """);
            statement.executeUpdate("""
              CREATE INDEX IF NOT EXISTS ix_transactions_position_date
              ON tax.transactions (account_id, instrument, tax_year, transaction_date, id);
            """);
            statement.executeUpdate("""
              CREATE INDEX IF NOT EXISTS ix_transactions_tax_year_position
              ON tax.transactions (tax_year, account_id, instrument, transaction_date);
            """);
            statement.executeUpdate("""
              CREATE INDEX IF NOT EXISTS ix_transactions_transaction_id
              ON tax.transactions (transaction_id);
            """);

            connection.commit();
        }
    }

    /**
     * Clear all test data for a specific tax year.
     */
    public void clearTestData(String taxYear) throws SQLException {
        try (var connection = DriverManager.getConnection(jdbcUrl, username, password);
             var statement = connection.createStatement()) {

            connection.setAutoCommit(false);
            statement.executeUpdate("DELETE FROM tax.opening_balances WHERE tax_year = '" + taxYear + "'");
            statement.executeUpdate("DELETE FROM tax.transactions WHERE tax_year = '" + taxYear + "'");
            connection.commit();
        }
    }

    /**
     * Insert a small set of test opening balances and transactions.
     */
    public void insertSmallTestDataset(String taxYear, long testId) throws SQLException {
        try (var connection = DriverManager.getConnection(jdbcUrl, username, password);
             var statement = connection.createStatement()) {

            connection.setAutoCommit(false);
            // Insert test opening balances
            statement.executeUpdate(String.format("""
                INSERT INTO tax.opening_balances (account_id, instrument, units_held, book_cost, tax_year)
                VALUES
                ('ACC%s01', 'AAPL', 100.00000000, 15000.00, '%s'),
                ('ACC%s02', 'MSFT', 50.00000000, 10000.00, '%s'),
                ('ACC%s03', 'GOOGL', 25.00000000, 7500.00, '%s')
            """, testId, taxYear, testId, taxYear, testId, taxYear));

            // Insert test transactions
            statement.executeUpdate(String.format("""
                INSERT INTO tax.transactions (transaction_id, account_id, instrument, transaction_type,
                                        transaction_date, units, price_per_unit, total_amount, tax_year)
                VALUES
                ('TX%s001', 'ACC%s01', 'AAPL', 'BUY', NOW(), 25.00000000, 155.00000000, 2.00, '%s'),
                ('TX%s002', 'ACC%s02', 'MSFT', 'BUY', NOW(), 20.00000000, 205.00000000, 2.00, '%s'),
                ('TX%s003', 'ACC%s03', 'GOOGL', 'SELL', NOW(), 10.00000000, 305.00000000, 2.00, '%s')
            """, testId, testId, taxYear, testId, testId, taxYear, testId, testId, taxYear));

            connection.commit();
        }
    }

    /**
     * Insert a larger test dataset that spans multiple windows.
     */
    public void insertMultiWindowTestDataset(String taxYear, long testId) throws SQLException {
        try (var connection = DriverManager.getConnection(jdbcUrl, username, password);
             var statement = connection.createStatement()) {

            connection.setAutoCommit(false);
            // Insert 5 opening balances (will create 2 windows with window size 3)
            statement.executeUpdate(String.format("""
                INSERT INTO tax.opening_balances (account_id, instrument, units_held, book_cost, tax_year)
                VALUES
                ('ACC%s01', 'AAPL', 100.00000000, 15000.00, '%s'),
                ('ACC%s02', 'MSFT', 50.00000000, 10000.00, '%s'),
                ('ACC%s03', 'GOOGL', 25.00000000, 7500.00, '%s'),
                ('ACC%s04', 'TSLA', 75.00000000, 22500.00, '%s'),
                ('ACC%s05', 'NVDA', 30.00000000, 12000.00, '%s')
            """, testId, taxYear, testId, taxYear, testId, taxYear, testId, taxYear, testId, taxYear));

            // Insert corresponding transactions
            statement.executeUpdate(String.format("""
                INSERT INTO tax.transactions (transaction_id, account_id, instrument, transaction_type,
                                        transaction_date, units, price_per_unit, total_amount, tax_year)
                VALUES
                ('TX%s01', 'ACC%s01', 'AAPL', 'BUY', NOW(), 10.00000000, 155.00000000, 2.00, '%s'),
                ('TX%s02', 'ACC%s05', 'NVDA', 'SELL', NOW(), 5.00000000, 405.00000000, 2.00, '%s')
            """, testId, testId, taxYear, testId, testId, taxYear));

            connection.commit();
        }
    }

    /**
     * Insert custom opening balances with specified data.
     */
    public void insertOpeningBalances(String taxYear, List<OpeningBalanceData> balances) throws SQLException {
        try (var connection = DriverManager.getConnection(jdbcUrl, username, password)) {
            connection.setAutoCommit(false);
            var sql = """
                INSERT INTO tax.opening_balances (account_id, instrument, units_held, book_cost, tax_year)
                VALUES (?, ?, ?, ?, ?)
            """;

            try (var preparedStatement = connection.prepareStatement(sql)) {
                for (var balance : balances) {
                    preparedStatement.setString(1, balance.accountId());
                    preparedStatement.setString(2, balance.instrumentId());
                    preparedStatement.setBigDecimal(3, balance.unitsHeld());
                    preparedStatement.setBigDecimal(4, balance.bookCost());
                    preparedStatement.setString(5, taxYear);
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
            }
            connection.commit();
        }
    }

    /**
     * Insert custom transactions with specified data.
     */
    public void insertTransactions(String taxYear, List<TransactionData> transactions) throws SQLException {
        try (var connection = DriverManager.getConnection(jdbcUrl, username, password)) {
            connection.setAutoCommit(false);
            var sql = """
                INSERT INTO tax.transactions (transaction_id, account_id, instrument, transaction_type,
                                        transaction_date, units, price_per_unit, total_amount, tax_year)
                VALUES (?, ?, ?, ?, NOW(), ?, ?,  ?)
            """;

            try (var preparedStatement = connection.prepareStatement(sql)) {
                for (var transaction : transactions) {
                    preparedStatement.setString(1, transaction.transactionId());
                    preparedStatement.setString(2, transaction.accountId());
                    preparedStatement.setString(3, transaction.instrumentId());
                    preparedStatement.setString(4, transaction.transactionType());
                    preparedStatement.setBigDecimal(5, transaction.units());
                    preparedStatement.setBigDecimal(6, transaction.unitPrice());
                    preparedStatement.setBigDecimal(7, transaction.totalAmount());
                    preparedStatement.setString(8, taxYear);
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
            }
            connection.commit();
        }
    }

    /**
     * Data record for opening balance test data.
     */
    public record OpeningBalanceData(
        String accountId,
        String instrumentId,
        java.math.BigDecimal unitsHeld,
        java.math.BigDecimal bookCost
    ) {}

    /**
     * Data record for transaction test data.
     */
    public record TransactionData(
        String transactionId,
        String accountId,
        String instrumentId,
        String transactionType,
        java.math.BigDecimal units,
        java.math.BigDecimal unitPrice,
        java.math.BigDecimal totalAmount
    ) {}
}