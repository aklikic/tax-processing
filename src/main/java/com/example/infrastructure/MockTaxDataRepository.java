package com.example.infrastructure;

import com.example.application.TaxDataRepository;
import com.example.domain.OpeningBalance;
import com.example.domain.PositionId;
import com.example.domain.Transaction;
import com.example.domain.TransactionType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Mock implementation of TaxDataRepository for development and testing.
 * Generates realistic synthetic data that follows expected patterns and constraints.
 * In production, this would be replaced with a real database implementation.
 *
 * Operating Modes:
 * - GENERATED_DATA: Uses auto-generated opening balances and transactions with pagination
 * - WINDOWED_TEST: Uses manually configured windows for precise test control
 */
public class MockTaxDataRepository implements TaxDataRepository {

    public enum Mode {
        GENERATED_DATA,  // Uses openingBalances/transactions lists with auto-generation
        WINDOWED_TEST    // Uses window maps with manual test data setup
    }

    private Mode currentMode;
    private final List<OpeningBalance> openingBalances;
    private final List<Transaction> transactions;
    private final Map<String, List<List<Transaction>>> transactionWindows;
    private final Map<String, Integer> windowCallCounts;
    private final Map<String, List<List<OpeningBalance>>> openingBalanceWindows;
    private final Map<String, Integer> openingBalanceWindowCallCounts;
    private String errorToThrow;

    public MockTaxDataRepository(int numPositions, int avgTransactionsPerPosition) {
        this.currentMode = Mode.GENERATED_DATA;
        this.openingBalances = generateOpeningBalances(numPositions);
        this.transactions = generateTransactions(openingBalances, avgTransactionsPerPosition);
        this.transactionWindows = new ConcurrentHashMap<>();
        this.windowCallCounts = new ConcurrentHashMap<>();
        this.openingBalanceWindows = new ConcurrentHashMap<>();
        this.openingBalanceWindowCallCounts = new ConcurrentHashMap<>();
    }

    /**
     * Constructor for test-specific configuration.
     */
    public MockTaxDataRepository() {
        this.currentMode = Mode.GENERATED_DATA; // Default mode, can be switched via setup methods
        this.openingBalances = new ArrayList<>();
        this.transactions = new ArrayList<>();
        this.transactionWindows = new ConcurrentHashMap<>();
        this.windowCallCounts = new ConcurrentHashMap<>();
        this.openingBalanceWindows = new ConcurrentHashMap<>();
        this.openingBalanceWindowCallCounts = new ConcurrentHashMap<>();
    }

    public static MockTaxDataRepository withDefaultData() {
        return new MockTaxDataRepository(1000, 3); // 1000 positions, 3 transactions each
    }

    public static MockTaxDataRepository withScaleTestData() {
        return new MockTaxDataRepository(5000, 3); // For testing full batch processing
    }

    @Override
    public List<OpeningBalance> loadOpeningBalancesBatch(String taxYear, int offset, int limit) {
        if (errorToThrow != null) {
            throw new RuntimeException(errorToThrow);
        }

        return switch (currentMode) {
            case WINDOWED_TEST -> {
                var windows = openingBalanceWindows.get(taxYear);
                if (windows != null) {
                    var windowIndex = openingBalanceWindowCallCounts.getOrDefault(taxYear, 0);
                    openingBalanceWindowCallCounts.put(taxYear, windowIndex + 1);

                    if (windowIndex < windows.size()) {
                        yield windows.get(windowIndex);
                    } else {
                        yield List.of();
                    }
                } else {
                    yield List.of(); // No windows configured for this tax year
                }
            }
            case GENERATED_DATA -> {
                if (offset >= openingBalances.size()) {
                    yield List.of();
                }
                int endIndex = Math.min(offset + limit, openingBalances.size());
                yield openingBalances.subList(offset, endIndex);
            }
        };
    }

    @Override
    public List<Transaction> loadTransactionsForPositions(List<PositionId> positionIds, String taxYear, int offset, int limit) {
        if (errorToThrow != null) {
            throw new RuntimeException(errorToThrow);
        }

        return switch (currentMode) {
            case WINDOWED_TEST -> {
                var windows = transactionWindows.get(taxYear);
                if (windows != null) {
                    var windowIndex = windowCallCounts.getOrDefault(taxYear, 0);
                    windowCallCounts.put(taxYear, windowIndex + 1);

                    if (windowIndex < windows.size()) {
                        yield windows.get(windowIndex);
                    } else {
                        yield List.of();
                    }
                } else {
                    yield List.of(); // No windows configured for this tax year
                }
            }
            case GENERATED_DATA -> {
                var filteredTransactions = transactions.stream()
                    .filter(tx -> positionIds.contains(tx.positionId()))
                    .sorted(Comparator.comparing(Transaction::dateTime).thenComparing(Transaction::id))
                    .toList();

                if (offset >= filteredTransactions.size()) {
                    yield List.of();
                }

                int endIndex = Math.min(offset + limit, filteredTransactions.size());
                yield filteredTransactions.subList(offset, endIndex);
            }
        };
    }

    @Override
    public long countOpeningBalances(String taxYear) {
        return openingBalances.size();
    }

    @Override
    public long countTransactionsForPositions(List<PositionId> positionIds, String taxYear) {
        return transactions.stream()
            .mapToLong(tx -> positionIds.contains(tx.positionId()) ? 1 : 0)
            .sum();
    }

    @Override
    public Flux<OpeningBalance> loadOpeningBalancesBatchFlux(String taxYear, int offset, int limit) {
        return Flux.fromIterable(loadOpeningBalancesBatch(taxYear, offset, limit));
    }

    @Override
    public Flux<Transaction> loadTransactionsForPositionsFlux(List<PositionId> positionIds, String taxYear, int offset, int limit) {
        return Flux.fromIterable(loadTransactionsForPositions(positionIds, taxYear, offset, limit));
    }

    @Override
    public Mono<Long> countOpeningBalancesMono(String taxYear) {
        return Mono.fromSupplier(() -> countOpeningBalances(taxYear));
    }

    @Override
    public Mono<Long> countTransactionsForPositionsMono(List<PositionId> positionIds, String taxYear) {
        return Mono.fromSupplier(() -> countTransactionsForPositions(positionIds, taxYear));
    }

    @Override
    public Flux<Transaction> loadTransactionsFlux(String taxYear, int offset, int limit) {
        // Load all transactions for the tax year without position filtering
        var allTransactions = transactions.stream()
            .sorted(Comparator.comparing(Transaction::dateTime).thenComparing(Transaction::id))
            .toList();

        if (offset >= allTransactions.size()) {
            return Flux.empty();
        }

        int endIndex = Math.min(offset + limit, allTransactions.size());
        return Flux.fromIterable(allTransactions.subList(offset, endIndex));
    }

    @Override
    public Mono<Long> countTransactionsMono(String taxYear) {
        return Mono.fromSupplier(() -> (long) transactions.size());
    }

    private List<OpeningBalance> generateOpeningBalances(int numPositions) {
        var balances = new ArrayList<OpeningBalance>();
        var instruments = List.of("AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "CRM");

        for (int i = 0; i < numPositions; i++) {
            var accountId = "ACC" + String.format("%06d", i / 8); // Multiple instruments per account
            var instrumentId = instruments.get(i % instruments.size());
            var openingUnits = BigDecimal.valueOf(100 + (i % 500)); // 100-600 units
            var openingCost = openingUnits.multiply(BigDecimal.valueOf(50 + (i % 200))); // $50-250 per unit

            balances.add(new OpeningBalance(accountId, instrumentId, openingUnits, openingCost));
        }

        // Sort by accountId, instrumentId for deterministic pagination
        balances.sort(Comparator.comparing(OpeningBalance::accountId)
                     .thenComparing(OpeningBalance::instrumentId));

        return balances;
    }

    private List<Transaction> generateTransactions(List<OpeningBalance> openingBalances, int avgTransactionsPerPosition) {
        var transactions = new ArrayList<Transaction>();
        var baseTime = Instant.parse("2023-01-01T00:00:00Z");

        int txIdCounter = 1;

        for (var balance : openingBalances) {
            var numTransactions = Math.max(1, avgTransactionsPerPosition + (txIdCounter % 3) - 1); // Vary 2-4 transactions

            for (int i = 0; i < numTransactions; i++) {
                var txId = "TX" + String.format("%08d", txIdCounter++);
                var dateTime = baseTime.plus(i * 30L + (txIdCounter % 365), ChronoUnit.DAYS);
                var type = (i % 3 == 0) ? TransactionType.BUY :
                          (i % 3 == 1) ? TransactionType.SELL : TransactionType.BUY;
                var units = BigDecimal.valueOf(10 + (i % 50)); // 10-60 units
                var price = BigDecimal.valueOf(45 + (txIdCounter % 100)); // $45-145 per unit
                var fees = BigDecimal.valueOf(5 + (i % 10)); // $5-15 fees

                transactions.add(new Transaction(
                    txId,
                    balance.accountId(),
                    balance.instrumentId(),
                    type,
                    dateTime,
                    units,
                    price,
                    fees
                ));
            }
        }

        // Sort by dateTime, txId for chronological order
        transactions.sort(Comparator.comparing(Transaction::dateTime)
                         .thenComparing(Transaction::id));

        return transactions;
    }

    /**
     * Get all opening balances for testing purposes.
     */
    public List<OpeningBalance> getAllOpeningBalances() {
        return List.copyOf(openingBalances);
    }

    /**
     * Get all transactions for testing purposes.
     */
    public List<Transaction> getAllTransactions() {
        return List.copyOf(transactions);
    }

    /**
     * Get transactions for a specific position for testing.
     */
    public List<Transaction> getTransactionsForPosition(PositionId positionId) {
        return transactions.stream()
            .filter(tx -> tx.positionId().equals(positionId))
            .sorted(Comparator.comparing(Transaction::dateTime))
            .toList();
    }

    /**
     * Setup specific transaction windows for testing.
     * Each list represents what should be returned for subsequent calls.
     * Switches to WINDOWED_TEST mode.
     */
    public void setupTransactions(String taxYear, List<List<Transaction>> windows) {
        this.currentMode = Mode.WINDOWED_TEST;
        transactionWindows.put(taxYear, windows);
        windowCallCounts.clear();
    }

    /**
     * Setup specific opening balance windows for testing.
     * Each list represents what should be returned for subsequent calls.
     * Switches to WINDOWED_TEST mode.
     */
    public void setupOpeningBalances(String taxYear, List<List<OpeningBalance>> windows) {
        this.currentMode = Mode.WINDOWED_TEST;
        openingBalanceWindows.put(taxYear, windows);
        openingBalanceWindowCallCounts.clear();

        // In windowed mode, flatten windows for count operations
        openingBalances.clear();
        for (var window : windows) {
            openingBalances.addAll(window);
        }
    }

    /**
     * Setup an error to be thrown when loading transactions.
     */
    public void setupError(String errorMessage) {
        this.errorToThrow = errorMessage;
    }

    /**
     * Clear any configured error.
     */
    public void clearError() {
        this.errorToThrow = null;
    }

    /**
     * Clear all test data and reset to GENERATED_DATA mode.
     */
    public void clearData() {
        this.currentMode = Mode.GENERATED_DATA;
        openingBalances.clear();
        transactions.clear();
        transactionWindows.clear();
        windowCallCounts.clear();
        openingBalanceWindows.clear();
        openingBalanceWindowCallCounts.clear();
        errorToThrow = null;
    }

    /**
     * Add opening balance to the generated data list.
     * Only works in GENERATED_DATA mode.
     */
    public void addOpeningBalance(OpeningBalance openingBalance) {
        if (currentMode != Mode.GENERATED_DATA) {
            throw new IllegalStateException("addOpeningBalance() only works in GENERATED_DATA mode. Current mode: " + currentMode);
        }
        openingBalances.add(openingBalance);
    }

    /**
     * Reset all test configuration but keep current mode.
     */
    public void reset() {
        transactionWindows.clear();
        windowCallCounts.clear();
        openingBalanceWindows.clear();
        openingBalanceWindowCallCounts.clear();
        errorToThrow = null;
    }

    /**
     * Get current operating mode for debugging.
     */
    public Mode getCurrentMode() {
        return currentMode;
    }
}