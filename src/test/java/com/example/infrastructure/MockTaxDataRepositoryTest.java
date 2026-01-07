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
 * Basic tests for MockTaxDataRepository focusing on mode switching and core functionality.
 * Mode-specific tests are in MockTaxDataRepositoryGeneratedModeTest and MockTaxDataRepositoryWindowedModeTest.
 */
public class MockTaxDataRepositoryTest {

    // === Basic Repository Creation Tests ===

    @Test
    public void shouldCreateEmptyRepositoryForTests() {
        var emptyRepo = new MockTaxDataRepository();

        assertThat(emptyRepo.getCurrentMode()).isEqualTo(MockTaxDataRepository.Mode.GENERATED_DATA);
        assertThat(emptyRepo.getAllOpeningBalances()).isEmpty();
        assertThat(emptyRepo.getAllTransactions()).isEmpty();
        assertThat(emptyRepo.countOpeningBalances("2023")).isZero();
        assertThat(emptyRepo.loadOpeningBalancesBatch("2023", 0, 10)).isEmpty();
    }

    @Test
    public void shouldCreateParameterizedRepository() {
        var repo = new MockTaxDataRepository(10, 2);

        assertThat(repo.getCurrentMode()).isEqualTo(MockTaxDataRepository.Mode.GENERATED_DATA);
        assertThat(repo.getAllOpeningBalances()).hasSize(10);
        assertThat(repo.getAllTransactions()).hasSizeGreaterThan(15); // ~2 per position with variation
        assertThat(repo.countOpeningBalances("2023")).isEqualTo(10);
    }

    // === Basic Window Setup Tests ===

    @Test
    public void shouldSetupBasicTransactionWindows() {
        var repo = new MockTaxDataRepository();
        assertThat(repo.getCurrentMode()).isEqualTo(MockTaxDataRepository.Mode.GENERATED_DATA);

        var transaction = new Transaction(
            "TX001", "ACC001", "AAPL", TransactionType.BUY,
            Instant.now(), BigDecimal.valueOf(100), BigDecimal.valueOf(150), BigDecimal.valueOf(5)
        );

        repo.setupTransactions("2023", List.of(List.of(transaction)));

        // Should switch to windowed test mode
        assertThat(repo.getCurrentMode()).isEqualTo(MockTaxDataRepository.Mode.WINDOWED_TEST);

        var result = repo.loadTransactionsForPositions(
            List.of(new PositionId("ACC001", "AAPL")), "2023", 0, 10
        );
        assertThat(result).containsExactly(transaction);
    }

    @Test
    public void shouldSetupBasicOpeningBalanceWindows() {
        var repo = new MockTaxDataRepository();
        assertThat(repo.getCurrentMode()).isEqualTo(MockTaxDataRepository.Mode.GENERATED_DATA);

        var balance = new OpeningBalance("ACC001", "AAPL", BigDecimal.valueOf(100), BigDecimal.valueOf(15000));
        repo.setupOpeningBalances("2023", List.of(List.of(balance)));

        // Should switch to windowed test mode
        assertThat(repo.getCurrentMode()).isEqualTo(MockTaxDataRepository.Mode.WINDOWED_TEST);

        var result = repo.loadOpeningBalancesBatch("2023", 0, 10);
        assertThat(result).containsExactly(balance);
    }

    // === Error and Reset Tests ===

    @Test
    public void shouldHandleErrorConfiguration() {
        var repo = new MockTaxDataRepository();
        var positionId = new PositionId("ACC001", "AAPL");

        repo.setupError("Database connection failed");

        assertThatThrownBy(() -> repo.loadTransactionsForPositions(
            List.of(positionId), "2023", 0, 10
        )).hasMessage("Database connection failed");

        // Clear error and verify it works again
        repo.clearError();

        var result = repo.loadTransactionsForPositions(
            List.of(positionId), "2023", 0, 10
        );
        assertThat(result).isEmpty(); // Empty repo, but no error
    }

    @Test
    public void shouldResetTestConfiguration() {
        var repo = new MockTaxDataRepository();

        // Setup some test data and switch to windowed mode
        repo.setupError("Test error");
        repo.setupTransactions("2023", List.of(List.of()));
        assertThat(repo.getCurrentMode()).isEqualTo(MockTaxDataRepository.Mode.WINDOWED_TEST);

        // Reset should clear error but preserve mode
        repo.reset();
        assertThat(repo.getCurrentMode()).isEqualTo(MockTaxDataRepository.Mode.WINDOWED_TEST);

        // Should not throw error after reset
        var result = repo.loadTransactionsForPositions(
            List.of(new PositionId("ACC001", "AAPL")), "2023", 0, 10
        );
        assertThat(result).isEmpty();
    }

    // === Factory Method Tests ===

    @Test
    public void shouldCreateDefaultDataRepository() {
        var defaultRepo = MockTaxDataRepository.withDefaultData();

        assertThat(defaultRepo.getCurrentMode()).isEqualTo(MockTaxDataRepository.Mode.GENERATED_DATA);
        assertThat(defaultRepo.countOpeningBalances("2023")).isEqualTo(1000);
        assertThat(defaultRepo.getAllTransactions()).hasSizeGreaterThan(2800); // ~3 per position
    }

    @Test
    public void shouldCreateScaleTestDataRepository() {
        var scaleRepo = MockTaxDataRepository.withScaleTestData();

        assertThat(scaleRepo.getCurrentMode()).isEqualTo(MockTaxDataRepository.Mode.GENERATED_DATA);
        assertThat(scaleRepo.countOpeningBalances("2023")).isEqualTo(5000);
        assertThat(scaleRepo.getAllTransactions()).hasSizeGreaterThan(14000); // ~3 per position
    }


    // === Reactive Method Compatibility Tests ===

    @Test
    public void shouldProvideConsistentDataBetweenSyncAndReactiveMethods() {
        var repo = new MockTaxDataRepository(20, 2); // Small dataset for testing
        var taxYear = "2023";

        // Test consistency between sync and reactive methods
        var syncOpeningCount = repo.countOpeningBalances(taxYear);
        var reactiveOpeningCount = repo.countOpeningBalancesMono(taxYear).block();
        assertThat(reactiveOpeningCount).isEqualTo(syncOpeningCount);

        var syncOpeningBalances = repo.loadOpeningBalancesBatch(taxYear, 0, 5);
        var reactiveOpeningBalances = repo.loadOpeningBalancesBatchFlux(taxYear, 0, 5)
            .collectList().block();
        assertThat(reactiveOpeningBalances).containsExactlyElementsOf(syncOpeningBalances);

        var allPositions = repo.getAllOpeningBalances().stream()
            .map(OpeningBalance::positionId)
            .limit(5)
            .toList();

        var syncTransactionCount = repo.countTransactionsForPositions(allPositions, taxYear);
        var reactiveTransactionCount = repo.countTransactionsForPositionsMono(allPositions, taxYear).block();
        assertThat(reactiveTransactionCount).isEqualTo(syncTransactionCount);

        var syncTransactions = repo.loadTransactionsForPositions(allPositions, taxYear, 0, 5);
        var reactiveTransactions = repo.loadTransactionsForPositionsFlux(allPositions, taxYear, 0, 5)
            .collectList().block();
        assertThat(reactiveTransactions).containsExactlyElementsOf(syncTransactions);
    }

    // === Mode Switching Tests ===

    @Test
    public void shouldStartInGeneratedDataMode() {
        var emptyRepo = new MockTaxDataRepository();
        assertThat(emptyRepo.getCurrentMode()).isEqualTo(MockTaxDataRepository.Mode.GENERATED_DATA);

        var paramRepo = new MockTaxDataRepository(10, 2);
        assertThat(paramRepo.getCurrentMode()).isEqualTo(MockTaxDataRepository.Mode.GENERATED_DATA);

        var defaultRepo = MockTaxDataRepository.withDefaultData();
        assertThat(defaultRepo.getCurrentMode()).isEqualTo(MockTaxDataRepository.Mode.GENERATED_DATA);

        var scaleRepo = MockTaxDataRepository.withScaleTestData();
        assertThat(scaleRepo.getCurrentMode()).isEqualTo(MockTaxDataRepository.Mode.GENERATED_DATA);
    }

    @Test
    public void shouldSwitchModesOnSetup() {
        var repo = new MockTaxDataRepository();
        assertThat(repo.getCurrentMode()).isEqualTo(MockTaxDataRepository.Mode.GENERATED_DATA);

        // Setting up transactions switches mode
        var transaction = new Transaction("TX001", "ACC001", "AAPL", TransactionType.BUY,
                Instant.now(), BigDecimal.valueOf(100), BigDecimal.valueOf(150), BigDecimal.TEN);
        repo.setupTransactions("2023", List.of(List.of(transaction)));
        assertThat(repo.getCurrentMode()).isEqualTo(MockTaxDataRepository.Mode.WINDOWED_TEST);

        // Clear and try with opening balances
        repo.clearData();
        assertThat(repo.getCurrentMode()).isEqualTo(MockTaxDataRepository.Mode.GENERATED_DATA);

        var balance = new OpeningBalance("ACC001", "AAPL", BigDecimal.valueOf(100), BigDecimal.valueOf(15000));
        repo.setupOpeningBalances("2023", List.of(List.of(balance)));
        assertThat(repo.getCurrentMode()).isEqualTo(MockTaxDataRepository.Mode.WINDOWED_TEST);
    }

    @Test
    public void shouldHandleModeTransitionsCorrectly() {
        var repo = new MockTaxDataRepository();

        // Start in generated mode
        assertThat(repo.getCurrentMode()).isEqualTo(MockTaxDataRepository.Mode.GENERATED_DATA);

        // Switch to windowed mode
        var transaction = new Transaction("TX001", "ACC001", "AAPL", TransactionType.BUY,
                Instant.now(), BigDecimal.valueOf(100), BigDecimal.valueOf(150), BigDecimal.TEN);
        repo.setupTransactions("2023", List.of(List.of(transaction)));
        assertThat(repo.getCurrentMode()).isEqualTo(MockTaxDataRepository.Mode.WINDOWED_TEST);

        // Reset preserves windowed mode
        repo.reset();
        assertThat(repo.getCurrentMode()).isEqualTo(MockTaxDataRepository.Mode.WINDOWED_TEST);

        // clearData resets to generated mode
        repo.clearData();
        assertThat(repo.getCurrentMode()).isEqualTo(MockTaxDataRepository.Mode.GENERATED_DATA);
    }

    @Test
    public void shouldEnforceModeRestrictions() {
        var repo = new MockTaxDataRepository();

        // Should work in generated data mode
        var balance = new OpeningBalance("ACC001", "AAPL", BigDecimal.valueOf(100), BigDecimal.valueOf(15000));
        repo.addOpeningBalance(balance);
        assertThat(repo.getAllOpeningBalances()).contains(balance);
        assertThat(repo.getCurrentMode()).isEqualTo(MockTaxDataRepository.Mode.GENERATED_DATA);

        // Switch to windowed test mode
        repo.setupTransactions("2023", List.of(List.of()));
        assertThat(repo.getCurrentMode()).isEqualTo(MockTaxDataRepository.Mode.WINDOWED_TEST);

        // Should throw exception when trying to add balance in windowed mode
        var anotherBalance = new OpeningBalance("ACC002", "MSFT", BigDecimal.valueOf(200), BigDecimal.valueOf(25000));
        assertThatThrownBy(() -> repo.addOpeningBalance(anotherBalance))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("addOpeningBalance() only works in GENERATED_DATA mode")
            .hasMessageContaining("Current mode: WINDOWED_TEST");
    }

    @Test
    public void shouldMaintainModeConsistencyThroughOperations() {
        var repo = new MockTaxDataRepository();

        // Start in generated mode and verify it persists through normal operations
        assertThat(repo.getCurrentMode()).isEqualTo(MockTaxDataRepository.Mode.GENERATED_DATA);
        repo.addOpeningBalance(new OpeningBalance("ACC001", "AAPL", BigDecimal.valueOf(100), BigDecimal.valueOf(15000)));
        repo.loadOpeningBalancesBatch("2023", 0, 10);
        repo.countOpeningBalances("2023");
        assertThat(repo.getCurrentMode()).isEqualTo(MockTaxDataRepository.Mode.GENERATED_DATA);

        // Switch to windowed mode and verify persistence
        repo.setupTransactions("2023", List.of(List.of()));
        assertThat(repo.getCurrentMode()).isEqualTo(MockTaxDataRepository.Mode.WINDOWED_TEST);
        repo.loadTransactionsForPositions(List.of(), "2023", 0, 10);
        repo.loadOpeningBalancesBatch("2024", 0, 10);
        assertThat(repo.getCurrentMode()).isEqualTo(MockTaxDataRepository.Mode.WINDOWED_TEST);

        // Test reset vs clearData behavior
        repo.reset(); // Preserves mode
        assertThat(repo.getCurrentMode()).isEqualTo(MockTaxDataRepository.Mode.WINDOWED_TEST);

        repo.clearData(); // Resets mode
        assertThat(repo.getCurrentMode()).isEqualTo(MockTaxDataRepository.Mode.GENERATED_DATA);
    }
}