package com.example.infrastructure;

import com.example.application.TaxDataRepository;
import com.example.domain.OpeningBalance;
import com.example.domain.PositionId;
import com.example.domain.Transaction;
import com.example.domain.TransactionType;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * PostgreSQL implementation of TaxDataRepository using R2DBC for reactive database access.
 * Provides efficient batch loading of opening balances and transactions with proper connection pooling.
 */
public class PostgreSQLTaxDataRepository implements TaxDataRepository {

    private static final Logger logger = LoggerFactory.getLogger(PostgreSQLTaxDataRepository.class);

    private final ConnectionFactory connectionFactory;

    public PostgreSQLTaxDataRepository(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    @Override
    public List<OpeningBalance> loadOpeningBalancesBatch(String taxYear, int offset, int limit) {
        logger.debug("Loading opening balances batch: taxYear={}, offset={}, limit={}", taxYear, offset, limit);

        var sql = """
            SELECT account_id, instrument, units_held, book_cost
            FROM tax.opening_balances
            WHERE tax_year = $1
            ORDER BY id
            LIMIT $2 OFFSET $3
            """;

        return loadOpeningBalancesBatchAsync(taxYear, offset, limit).join();
    }

    @Override
    public List<Transaction> loadTransactionsForPositions(List<PositionId> positionIds, String taxYear, int offset, int limit) {
        if (positionIds.isEmpty()) {
            return List.of();
        }

        logger.debug("Loading transactions for positions: count={}, taxYear={}, offset={}, limit={}",
                    positionIds.size(), taxYear, offset, limit);

        return loadTransactionsForPositionsAsync(positionIds, taxYear, offset, limit).join();
    }

    @Override
    public long countOpeningBalances(String taxYear) {
        logger.debug("Counting opening balances for taxYear={}", taxYear);
        return countOpeningBalancesAsync(taxYear).join();
    }

    @Override
    public long countTransactionsForPositions(List<PositionId> positionIds, String taxYear) {
        if (positionIds.isEmpty()) {
            return 0L;
        }

        logger.debug("Counting transactions for positions: count={}, taxYear={}", positionIds.size(), taxYear);
        return countTransactionsForPositionsAsync(positionIds, taxYear).join();
    }

    /**
     * Async version of loadOpeningBalancesBatch returning CompletableFuture.
     */
    public CompletableFuture<List<OpeningBalance>> loadOpeningBalancesBatchAsync(String taxYear, int offset, int limit) {
        var sql = """
            SELECT account_id, instrument, units_held, book_cost
            FROM tax.opening_balances
            WHERE tax_year = $1
            ORDER BY id
            LIMIT $2 OFFSET $3
            """;

        return Mono.usingWhen(
                connectionFactory.create(),
                connection -> {
                    var statement = connection.createStatement(sql);
                    statement.bind(0, taxYear);
                    statement.bind(1, limit);
                    statement.bind(2, offset);

                    return Flux.from(statement.execute())
                        .flatMap(result -> result.map(this::mapToOpeningBalance))
                        .collectList();
                },
                connection -> connection.close()
            )
            .doOnError(e -> logger.error("Failed to load opening balances batch", e))
            .onErrorMap(e -> new RuntimeException("Database error loading opening balances: " + e.getMessage(), e))
            .toFuture();
    }

    /**
     * Async version of loadTransactionsForPositions returning CompletableFuture.
     */
    public CompletableFuture<List<Transaction>> loadTransactionsForPositionsAsync(List<PositionId> positionIds, String taxYear, int offset, int limit) {
        // Create position filter strings
        var positionFilters = positionIds.stream()
            .map(pos -> pos.accountId() + "|" + pos.instrumentId())
            .toArray(String[]::new);

        var sql = """
            SELECT transaction_id, account_id, instrument, transaction_date,
                   transaction_type, units, price_per_unit, total_amount
            FROM tax.transactions
            WHERE tax_year = $1
            AND (account_id || '|' || instrument) = ANY($2::text[])
            ORDER BY account_id, instrument, transaction_date, id
            LIMIT $3 OFFSET $4
            """;

        return Mono.usingWhen(
                connectionFactory.create(),
                connection -> {
                    var statement = connection.createStatement(sql);
                    statement.bind(0, taxYear);
                    statement.bind(1, positionFilters);
                    statement.bind(2, limit);
                    statement.bind(3, offset);

                    return Flux.from(statement.execute())
                        .flatMap(result -> result.map(this::mapToTransaction))
                        .collectList();
                },
                connection -> connection.close()
            )
            .doOnError(e -> logger.error("Failed to load transactions for positions", e))
            .onErrorMap(e -> new RuntimeException("Database error loading transactions: " + e.getMessage(), e))
            .toFuture();
    }

    /**
     * Async version of countOpeningBalances returning CompletableFuture.
     */
    public CompletableFuture<Long> countOpeningBalancesAsync(String taxYear) {
        var sql = "SELECT COUNT(*) FROM tax.opening_balances WHERE tax_year = $1";

        return Mono.usingWhen(
                connectionFactory.create(),
                connection -> {
                    var statement = connection.createStatement(sql);
                    statement.bind(0, taxYear);

                    return Flux.from(statement.execute())
                        .flatMap(result -> result.map((row, metadata) -> row.get(0, Long.class)))
                        .next()
                        .defaultIfEmpty(0L);
                },
                connection -> connection.close()
            )
            .doOnError(e -> logger.error("Failed to count opening balances", e))
            .onErrorReturn(0L)
            .toFuture();
    }

    /**
     * Async version of countTransactionsForPositions returning CompletableFuture.
     */
    public CompletableFuture<Long> countTransactionsForPositionsAsync(List<PositionId> positionIds, String taxYear) {
        // Create position filter strings
        var positionFilters = positionIds.stream()
            .map(pos -> pos.accountId() + "|" + pos.instrumentId())
            .toArray(String[]::new);

        var sql = """
            SELECT COUNT(*)
            FROM tax.transactions
            WHERE tax_year = $1
            AND (account_id || '|' || instrument) = ANY($2::text[])
            """;

        return Mono.usingWhen(
                connectionFactory.create(),
                connection -> {
                    var statement = connection.createStatement(sql);
                    statement.bind(0, taxYear);
                    statement.bind(1, positionFilters);

                    return Flux.from(statement.execute())
                        .flatMap(result -> result.map((row, metadata) -> row.get(0, Long.class)))
                        .next()
                        .defaultIfEmpty(0L);
                },
                connection -> connection.close()
            )
            .doOnError(e -> logger.error("Failed to count transactions for positions", e))
            .onErrorReturn(0L)
            .toFuture();
    }

    /**
     * Maps a database row to an OpeningBalance domain object.
     */
    private OpeningBalance mapToOpeningBalance(Row row, io.r2dbc.spi.RowMetadata metadata) {
        try {
            var accountId = row.get("account_id", String.class);
            var instrumentId = row.get("instrument", String.class);
            var unitsHeld = row.get("units_held", BigDecimal.class);
            var bookCost = row.get("book_cost", BigDecimal.class);

            return new OpeningBalance(accountId, instrumentId, unitsHeld, bookCost);
        } catch (Exception e) {
            logger.error("Failed to map opening balance row: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to map opening balance row", e);
        }
    }

    /**
     * Maps a database row to a Transaction domain object.
     */
    private Transaction mapToTransaction(Row row, io.r2dbc.spi.RowMetadata metadata) {
        try {
            var transactionId = row.get("transaction_id", String.class);
            var accountId = row.get("account_id", String.class);
            var instrumentId = row.get("instrument", String.class);
            var transactionTypeStr = row.get("transaction_type", String.class);
            var transactionDate = row.get("transaction_date", OffsetDateTime.class);
            var units = row.get("units", BigDecimal.class);
            var pricePerUnit = row.get("price_per_unit", BigDecimal.class);
            var totalAmount = row.get("total_amount", BigDecimal.class);

            // Map string to TransactionType enum
            var transactionType = mapToTransactionType(transactionTypeStr);

            // Convert OffsetDateTime to Instant
            var instant = transactionDate.toInstant();

            // Calculate fees: totalAmount should be (units * price) + fees for BUY
            // or (units * price) - fees for SELL
            var grossAmount = units.multiply(pricePerUnit);
            var fees = totalAmount.subtract(grossAmount).abs();

            return new Transaction(
                transactionId,
                accountId,
                instrumentId,
                transactionType,
                instant,
                units.abs(), // Ensure units are positive
                pricePerUnit,
                fees
            );
        } catch (Exception e) {
            logger.error("Failed to map transaction row: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to map transaction row", e);
        }
    }

    /**
     * Maps database transaction type string to TransactionType enum.
     */
    private TransactionType mapToTransactionType(String typeString) {
        return switch (typeString.toUpperCase()) {
            case "BUY" -> TransactionType.BUY;
            case "SELL" -> TransactionType.SELL;
            case "DIVIDEND" -> TransactionType.CORPORATE_ACTION;
            case "SPLIT" -> TransactionType.CORPORATE_ACTION;
            case "SPINOFF" -> TransactionType.CORPORATE_ACTION;
            case "TRANSFER_IN" -> TransactionType.TRANSFER_IN;
            case "TRANSFER_OUT" -> TransactionType.TRANSFER_OUT;
            default -> {
                logger.warn("Unknown transaction type: {}, defaulting to CORPORATE_ACTION", typeString);
                yield TransactionType.CORPORATE_ACTION;
            }
        };
    }
}