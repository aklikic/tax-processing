package com.example.infrastructure;

import com.example.domain.OpeningBalance;
import com.example.domain.PositionId;
import com.example.domain.Transaction;
import com.example.domain.TransactionType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for MockTaxDataRepository.
 * Tests data generation, pagination, and test-specific configuration.
 */
public class MockTaxDataRepositoryTest {

    private MockTaxDataRepository repository;

    @BeforeEach
    void setUp() {
        repository = new MockTaxDataRepository(100, 3); // 100 positions, 3 transactions each
    }

    @Test
    public void shouldGenerateCorrectNumberOfOpeningBalances() {
        var balances = repository.getAllOpeningBalances();

        assertThat(balances).hasSize(100);

        // Verify all have required fields
        balances.forEach(balance -> {
            assertThat(balance.accountId()).isNotBlank();
            assertThat(balance.instrumentId()).isNotBlank();
            assertThat(balance.openingUnits()).isGreaterThanOrEqualTo(BigDecimal.ZERO);
            assertThat(balance.openingCost()).isGreaterThanOrEqualTo(BigDecimal.ZERO);
        });
    }

    @Test
    public void shouldGenerateTransactionsForAllPositions() {
        var transactions = repository.getAllTransactions();

        // Should have approximately 3 transactions per position (some variation)
        assertThat(transactions).hasSizeBetween(280, 320); // 100 * 3 with variation

        // Verify all transactions have required fields
        transactions.forEach(tx -> {
            assertThat(tx.id()).isNotBlank();
            assertThat(tx.accountId()).isNotBlank();
            assertThat(tx.instrumentId()).isNotBlank();
            assertThat(tx.type()).isNotNull();
            assertThat(tx.dateTime()).isNotNull();
            assertThat(tx.units()).isGreaterThan(BigDecimal.ZERO);
            assertThat(tx.price()).isGreaterThan(BigDecimal.ZERO);
            assertThat(tx.fees()).isGreaterThanOrEqualTo(BigDecimal.ZERO);
        });
    }

    @Test
    public void shouldLoadOpeningBalancesBatchWithPagination() {
        var taxYear = "2023";

        // First page
        var firstBatch = repository.loadOpeningBalancesBatch(taxYear, 0, 20);
        assertThat(firstBatch).hasSize(20);

        // Second page
        var secondBatch = repository.loadOpeningBalancesBatch(taxYear, 20, 20);
        assertThat(secondBatch).hasSize(20);

        // Verify no overlap
        var firstIds = firstBatch.stream().map(b -> b.positionId().toEntityId()).toList();
        var secondIds = secondBatch.stream().map(b -> b.positionId().toEntityId()).toList();
        assertThat(firstIds).doesNotContainAnyElementsOf(secondIds);

        // Last page with remaining items
        var lastBatch = repository.loadOpeningBalancesBatch(taxYear, 80, 30);
        assertThat(lastBatch).hasSize(20); // Only 20 remaining

        // Out of range
        var emptyBatch = repository.loadOpeningBalancesBatch(taxYear, 200, 20);
        assertThat(emptyBatch).isEmpty();
    }

    @Test
    public void shouldLoadTransactionsForSpecificPositions() {
        var balances = repository.getAllOpeningBalances();
        var firstTwoPositions = balances.subList(0, 2).stream()
            .map(OpeningBalance::positionId)
            .toList();

        var transactions = repository.loadTransactionsForPositions(
            firstTwoPositions, "2023", 0, 10
        );

        // Should only return transactions for the specified positions
        transactions.forEach(tx -> {
            assertThat(firstTwoPositions).contains(tx.positionId());
        });

        // Verify chronological order
        for (int i = 1; i < transactions.size(); i++) {
            var prevDateTime = transactions.get(i - 1).dateTime();
            var currDateTime = transactions.get(i).dateTime();
            assertThat(currDateTime).isAfterOrEqualTo(prevDateTime);
        }
    }

    @Test
    public void shouldCountOpeningBalances() {
        var count = repository.countOpeningBalances("2023");
        assertThat(count).isEqualTo(100);
    }

    @Test
    public void shouldCountTransactionsForPositions() {
        var balances = repository.getAllOpeningBalances();
        var allPositions = balances.stream()
            .map(OpeningBalance::positionId)
            .toList();

        var count = repository.countTransactionsForPositions(allPositions, "2023");

        // Should match the size of all transactions
        assertThat(count).isEqualTo(repository.getAllTransactions().size());
    }

    @Test
    public void shouldGenerateDataWithDifferentInstruments() {
        var balances = repository.getAllOpeningBalances();

        // Should have multiple different instruments
        var uniqueInstruments = balances.stream()
            .map(OpeningBalance::instrumentId)
            .distinct()
            .toList();

        assertThat(uniqueInstruments).hasSizeGreaterThan(5); // At least 6 different instruments
        assertThat(uniqueInstruments).contains("AAPL", "MSFT", "GOOGL");
    }

    @Test
    public void shouldGenerateMultipleInstrumentsPerAccount() {
        var balances = repository.getAllOpeningBalances();

        // Group by account and check that some accounts have multiple instruments
        var accountsWithMultipleInstruments = balances.stream()
            .collect(java.util.stream.Collectors.groupingBy(OpeningBalance::accountId))
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue().size() > 1)
            .count();

        assertThat(accountsWithMultipleInstruments).isGreaterThan(0);
    }

    @Test
    public void shouldGenerateDeterministicData() {
        // Create two repositories with same parameters
        var repo1 = new MockTaxDataRepository(50, 2);
        var repo2 = new MockTaxDataRepository(50, 2);

        var balances1 = repo1.getAllOpeningBalances();
        var balances2 = repo2.getAllOpeningBalances();

        // Should generate identical data for same parameters
        assertThat(balances1).hasSize(balances2.size());
        for (int i = 0; i < balances1.size(); i++) {
            var b1 = balances1.get(i);
            var b2 = balances2.get(i);
            assertThat(b1.accountId()).isEqualTo(b2.accountId());
            assertThat(b1.instrumentId()).isEqualTo(b2.instrumentId());
            assertThat(b1.openingUnits()).isEqualTo(b2.openingUnits());
            assertThat(b1.openingCost()).isEqualTo(b2.openingCost());
        }
    }

    @Test
    public void shouldSortOpeningBalancesByAccountAndInstrument() {
        var balances = repository.loadOpeningBalancesBatch("2023", 0, 50);

        // Verify sorted order
        for (int i = 1; i < balances.size(); i++) {
            var prev = balances.get(i - 1);
            var curr = balances.get(i);

            int accountComparison = prev.accountId().compareTo(curr.accountId());
            if (accountComparison == 0) {
                // Same account, instrument should be in order
                assertThat(prev.instrumentId()).isLessThanOrEqualTo(curr.instrumentId());
            } else {
                // Different account, account should be in order
                assertThat(accountComparison).isLessThan(0);
            }
        }
    }

    @Test
    public void shouldCreateEmptyRepositoryForTests() {
        var emptyRepo = new MockTaxDataRepository();

        assertThat(emptyRepo.getAllOpeningBalances()).isEmpty();
        assertThat(emptyRepo.getAllTransactions()).isEmpty();
        assertThat(emptyRepo.countOpeningBalances("2023")).isZero();
        assertThat(emptyRepo.loadOpeningBalancesBatch("2023", 0, 10)).isEmpty();
    }

    @Test
    public void shouldSetupCustomTransactionWindows() {
        var emptyRepo = new MockTaxDataRepository();
        var positionId = new PositionId("ACC001", "AAPL");

        var transaction1 = new Transaction(
            "TX001", "ACC001", "AAPL", TransactionType.BUY,
            Instant.now(), BigDecimal.valueOf(100), BigDecimal.valueOf(150), BigDecimal.valueOf(5)
        );
        var transaction2 = new Transaction(
            "TX002", "ACC001", "AAPL", TransactionType.SELL,
            Instant.now(), BigDecimal.valueOf(50), BigDecimal.valueOf(160), BigDecimal.valueOf(3)
        );

        // Setup windows
        emptyRepo.setupTransactions("2023", List.of(
            List.of(transaction1),
            List.of(transaction2),
            List.of() // Empty window
        ));

        // First call should return first window
        var firstWindow = emptyRepo.loadTransactionsForPositions(
            List.of(positionId), "2023", 0, 10
        );
        assertThat(firstWindow).containsExactly(transaction1);

        // Second call should return second window
        var secondWindow = emptyRepo.loadTransactionsForPositions(
            List.of(positionId), "2023", 10, 10
        );
        assertThat(secondWindow).containsExactly(transaction2);

        // Third call should return empty
        var thirdWindow = emptyRepo.loadTransactionsForPositions(
            List.of(positionId), "2023", 20, 10
        );
        assertThat(thirdWindow).isEmpty();
    }

    @Test
    public void shouldThrowConfiguredError() {
        var emptyRepo = new MockTaxDataRepository();
        var positionId = new PositionId("ACC001", "AAPL");

        emptyRepo.setupError("Database connection failed");

        assertThatThrownBy(() -> emptyRepo.loadTransactionsForPositions(
            List.of(positionId), "2023", 0, 10
        )).hasMessage("Database connection failed");

        // Clear error and verify it works again
        emptyRepo.clearError();

        var result = emptyRepo.loadTransactionsForPositions(
            List.of(positionId), "2023", 0, 10
        );
        assertThat(result).isEmpty(); // Empty repo, but no error
    }

    @Test
    public void shouldResetTestConfiguration() {
        var emptyRepo = new MockTaxDataRepository();
        var positionId = new PositionId("ACC001", "AAPL");

        // Setup some test data
        emptyRepo.setupError("Test error");
        emptyRepo.setupTransactions("2023", List.of(List.of()));

        // Reset
        emptyRepo.reset();

        // Should not throw error after reset
        var result = emptyRepo.loadTransactionsForPositions(
            List.of(positionId), "2023", 0, 10
        );
        assertThat(result).isEmpty();
    }

    @Test
    public void shouldCreateDefaultDataRepository() {
        var defaultRepo = MockTaxDataRepository.withDefaultData();

        assertThat(defaultRepo.countOpeningBalances("2023")).isEqualTo(1000);
        assertThat(defaultRepo.getAllTransactions()).hasSizeGreaterThan(2800); // ~3 per position
    }

    @Test
    public void shouldCreateScaleTestDataRepository() {
        var scaleRepo = MockTaxDataRepository.withScaleTestData();

        assertThat(scaleRepo.countOpeningBalances("2023")).isEqualTo(5000);
        assertThat(scaleRepo.getAllTransactions()).hasSizeGreaterThan(14000); // ~3 per position
    }

    @Test
    public void shouldGetTransactionsForSpecificPosition() {
        var balances = repository.getAllOpeningBalances();
        var firstPosition = balances.get(0).positionId();

        var positionTransactions = repository.getTransactionsForPosition(firstPosition);

        // All transactions should belong to this position
        positionTransactions.forEach(tx -> {
            assertThat(tx.positionId()).isEqualTo(firstPosition);
        });

        // Should be sorted by date
        for (int i = 1; i < positionTransactions.size(); i++) {
            assertThat(positionTransactions.get(i).dateTime())
                .isAfterOrEqualTo(positionTransactions.get(i - 1).dateTime());
        }
    }

    @Test
    public void shouldGenerateReasonableFinancialValues() {
        var balances = repository.getAllOpeningBalances();
        var transactions = repository.getAllTransactions();

        // Opening balances should have reasonable values
        balances.forEach(balance -> {
            assertThat(balance.openingUnits()).isBetween(BigDecimal.valueOf(100), BigDecimal.valueOf(600));
            var avgCostPerUnit = balance.openingCost().divide(balance.openingUnits(), 2, java.math.RoundingMode.HALF_UP);
            assertThat(avgCostPerUnit).isBetween(BigDecimal.valueOf(50), BigDecimal.valueOf(250));
        });

        // Transactions should have reasonable values
        transactions.forEach(tx -> {
            assertThat(tx.units()).isBetween(BigDecimal.valueOf(10), BigDecimal.valueOf(60));
            assertThat(tx.price()).isBetween(BigDecimal.valueOf(45), BigDecimal.valueOf(145));
            assertThat(tx.fees()).isBetween(BigDecimal.valueOf(5), BigDecimal.valueOf(15));
        });
    }

    @Test
    public void shouldGenerateVariousTransactionTypes() {
        var transactions = repository.getAllTransactions();

        var transactionTypes = transactions.stream()
            .map(Transaction::type)
            .distinct()
            .toList();

        // Should have BUY and SELL at minimum
        assertThat(transactionTypes).contains(TransactionType.BUY, TransactionType.SELL);
    }
}