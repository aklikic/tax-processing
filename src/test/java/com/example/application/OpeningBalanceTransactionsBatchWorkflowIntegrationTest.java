package com.example.application;

import akka.Done;
import akka.javasdk.DependencyProvider;
import akka.javasdk.testkit.TestKit;
import akka.javasdk.testkit.TestKitSupport;
import com.example.domain.*;
import com.example.infrastructure.MockTaxDataRepository;
import com.typesafe.config.ConfigFactory;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for OpeningBalanceTransactionsBatch workflow.
 * Tests the complete workflow execution including position initialization,
 * transaction processing, and state management.
 */
public class OpeningBalanceTransactionsBatchWorkflowIntegrationTest extends TestKitSupport {

    private final MockTaxDataRepository mockRepository = new MockTaxDataRepository();

    @Override
    protected TestKit.Settings testKitSettings() {
        // Create test configuration
        var testConfig = ConfigFactory.parseString("""
            tax-processing {
                positions-per-window = 5000
                position-init-batch-size = 500
                transaction-microbatch-size = 111
                transaction-window-size = 10
                max-parallel-sub-workflows = 45
                completion-window = 5
                emergency-threshold = 10
                position-idempotency-cache-size = 1000
                max-parallel-windows = 3
            }
            """);

        var processingConfig = ProcessingConfig.fromConfig(testConfig);

        var dependencyProvider = new DependencyProvider() {
            @SuppressWarnings("unchecked")
            @Override
            public <T> T getDependency(Class<T> clazz) {
                if (clazz.equals(TaxDataRepository.class)) {
                    return (T) mockRepository;
                } else if (clazz.equals(ProcessingConfig.class)) {
                    return (T) processingConfig;
                } else {
                    throw new IllegalArgumentException("Unknown dependency type: " + clazz);
                }
            }
        };

        return TestKit.Settings.DEFAULT.withDependencyProvider(dependencyProvider);
    }

    @Test
    public void shouldProcessTransactionBatchSuccessfully() {
        // Setup test data
        var positionId1 = new PositionId("ACC001", "AAPL");
        var positionId2 = new PositionId("ACC002", "MSFT");
        var positionIds = List.of(positionId1, positionId2);
        var taxYear = "2023";
        var batchId = "batch-001";

        // Initialize positions first
        initializePosition(positionId1, BigDecimal.valueOf(100), BigDecimal.valueOf(15000)); // $150/unit
        initializePosition(positionId2, BigDecimal.valueOf(50), BigDecimal.valueOf(10000));  // $200/unit

        // Setup mock transactions - split into windows of 2 (transaction-window-size)
        var transaction1 = new Transaction(
            "TX001", "ACC001", "AAPL", TransactionType.BUY,
            Instant.now(), BigDecimal.valueOf(25), BigDecimal.valueOf(155), BigDecimal.valueOf(5)
        );
        var transaction2 = new Transaction(
            "TX002", "ACC002", "MSFT", TransactionType.BUY,
            Instant.now(), BigDecimal.valueOf(20), BigDecimal.valueOf(205), BigDecimal.valueOf(8)
        );
        var transaction3 = new Transaction(
            "TX003", "ACC001", "AAPL", TransactionType.SELL,
            Instant.now(), BigDecimal.valueOf(10), BigDecimal.valueOf(160), BigDecimal.valueOf(3)
        );

        // Mock repository to return transactions in windows
        mockRepository.setupTransactions(taxYear, List.of(
            // First window (offset 0, limit 2)
            List.of(transaction1, transaction2),
            // Second window (offset 2, limit 2)
            List.of(transaction3),
            // Third window (offset 3, limit 2) - empty
            List.of()
        ));

        // Start the workflow
        var parentWorkflowId = "parent-workflow-" + batchId;
        var workflowId = "workflow-" + batchId;
        var startCommand = new OpeningBalanceTransactionsBatchWorkflow.StartCommand(
            batchId, taxYear, positionIds, parentWorkflowId
        );

        var startResult = componentClient.forWorkflow(workflowId)
            .method(OpeningBalanceTransactionsBatchWorkflow::start)
            .invoke(startCommand);

        assertThat(startResult).isEqualTo(Done.getInstance());

        // Wait for workflow completion with generous timeout for async processing
        Awaitility.await()
            .atMost(30, TimeUnit.SECONDS)
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted(() -> {
                var status = componentClient.forWorkflow(workflowId)
                    .method(OpeningBalanceTransactionsBatchWorkflow::getStatus)
                    .invoke();

                assertThat(status.status()).isEqualTo(
                    OpeningBalanceTransactionsBatchState.ProcessingStatus.COMPLETED
                );
                assertThat(status.processedTransactions()).isEqualTo(3);
                assertThat(status.errorMessage()).isNull();
            });

        // Verify final position states
        // positionId1: 100 units @ $150/unit = $15,000
        // + BUY 25 @ $155 + $5 fees = +$3,880 → 125 units, $18,880 ($151.04/unit)
        // - SELL 10 @ $160 - $3 fees = remove $1,510.40 cost basis → 115 units, $17,369.60
        verifyPositionState(positionId1, BigDecimal.valueOf(115), BigDecimal.valueOf(17369.60));

        // positionId2: 50 units @ $200/unit = $10,000
        // + BUY 20 @ $205 + $8 fees = +$4,108 → 70 units, $14,108 ($201.54/unit)
        verifyPositionState(positionId2, BigDecimal.valueOf(70), BigDecimal.valueOf(14108));
    }

    @Test
    public void shouldHandleEmptyTransactionBatch() {
        var positionIds = List.of(new PositionId("ACC003", "GOOGL"));
        var taxYear = "2023";
        var batchId = "batch-empty";
        var workflowId = "workflow-" + batchId;

        // Initialize position
        initializePosition(positionIds.get(0), BigDecimal.valueOf(50), BigDecimal.valueOf(8000));

        // Mock repository to return no transactions
        mockRepository.setupTransactions(taxYear, List.of(List.of()));

        var parentWorkflowId = "parent-workflow-" + batchId;
        var startCommand = new OpeningBalanceTransactionsBatchWorkflow.StartCommand(
                batchId, taxYear, positionIds, parentWorkflowId
        );

        componentClient.forWorkflow(workflowId)
            .method(OpeningBalanceTransactionsBatchWorkflow::start)
            .invoke(startCommand);

        // Workflow should complete immediately with no transactions
        Awaitility.await()
            .atMost(10, TimeUnit.SECONDS)
            .untilAsserted(() -> {
                var status = componentClient.forWorkflow(workflowId)
                    .method(OpeningBalanceTransactionsBatchWorkflow::getStatus)
                    .invoke();

                assertThat(status.status()).isEqualTo(
                    OpeningBalanceTransactionsBatchState.ProcessingStatus.COMPLETED
                );
                assertThat(status.processedTransactions()).isEqualTo(0);
            });
    }

    @Test
    public void shouldHandleRepositoryFailure() {
        var positionIds = List.of(new PositionId("ACC004", "TSLA"));
        var taxYear = "2023";
        var batchId = "batch-error";
        var workflowId = "workflow-" + batchId;

        // Initialize position
        initializePosition(positionIds.get(0), BigDecimal.valueOf(30), BigDecimal.valueOf(9000));

        // Mock repository to throw error
        mockRepository.setupError("Database connection failed");

        var parentWorkflowId = "parent-workflow-" + batchId;
        var startCommand = new OpeningBalanceTransactionsBatchWorkflow.StartCommand(
                batchId, taxYear, positionIds, parentWorkflowId
        );

        componentClient.forWorkflow(workflowId)
            .method(OpeningBalanceTransactionsBatchWorkflow::start)
            .invoke(startCommand);

        // Workflow should fail and transition to error handling
        Awaitility.await()
            .atMost(15, TimeUnit.SECONDS)
            .untilAsserted(() -> {
                var status = componentClient.forWorkflow(workflowId)
                    .method(OpeningBalanceTransactionsBatchWorkflow::getStatus)
                    .invoke();

                assertThat(status.status()).isEqualTo(
                    OpeningBalanceTransactionsBatchState.ProcessingStatus.FAILED
                );
            });

        // Clear error for other tests
        mockRepository.clearError();
    }

    @Test
    public void shouldProcessMultipleTransactionWindows() {
        var positionId = new PositionId("ACC005", "NVDA");
        var positionIds = List.of(positionId);
        var taxYear = "2023";
        var batchId = "batch-multi-window";
        var workflowId = "workflow-" + batchId;

        // Initialize position
        initializePosition(positionId, BigDecimal.valueOf(200), BigDecimal.valueOf(40000)); // $200/unit

        // Create 5 transactions across multiple windows (window size = 2)
        var transactions = List.of(
            new Transaction("TX1", "ACC005", "NVDA", TransactionType.BUY,
                Instant.now(), BigDecimal.valueOf(10), BigDecimal.valueOf(210), BigDecimal.valueOf(5)),
            new Transaction("TX2", "ACC005", "NVDA", TransactionType.BUY,
                Instant.now(), BigDecimal.valueOf(15), BigDecimal.valueOf(220), BigDecimal.valueOf(7)),
            new Transaction("TX3", "ACC005", "NVDA", TransactionType.SELL,
                Instant.now(), BigDecimal.valueOf(25), BigDecimal.valueOf(230), BigDecimal.valueOf(6)),
            new Transaction("TX4", "ACC005", "NVDA", TransactionType.BUY,
                Instant.now(), BigDecimal.valueOf(5), BigDecimal.valueOf(240), BigDecimal.valueOf(3)),
            new Transaction("TX5", "ACC005", "NVDA", TransactionType.CORPORATE_ACTION,
                Instant.now(), BigDecimal.valueOf(205), BigDecimal.valueOf(2), BigDecimal.ZERO) // $2 dividend per unit
        );

        // Setup transactions across multiple windows
        mockRepository.setupTransactions(taxYear, List.of(
            // Window 1: TX1, TX2
            List.of(transactions.get(0), transactions.get(1)),
            // Window 2: TX3, TX4
            List.of(transactions.get(2), transactions.get(3)),
            // Window 3: TX5
            List.of(transactions.get(4)),
            // Window 4: empty
            List.of()
        ));

        var parentWorkflowId = "parent-workflow-" + batchId;
        var startCommand = new OpeningBalanceTransactionsBatchWorkflow.StartCommand(
                batchId, taxYear, positionIds, parentWorkflowId
        );

        componentClient.forWorkflow(workflowId)
            .method(OpeningBalanceTransactionsBatchWorkflow::start)
            .invoke(startCommand);

        // Wait for all windows to be processed
        Awaitility.await()
            .atMost(45, TimeUnit.SECONDS) // Longer timeout for multiple windows
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted(() -> {
                var status = componentClient.forWorkflow(workflowId)
                    .method(OpeningBalanceTransactionsBatchWorkflow::getStatus)
                    .invoke();

                assertThat(status.status()).isEqualTo(
                    OpeningBalanceTransactionsBatchState.ProcessingStatus.COMPLETED
                );
                assertThat(status.processedTransactions()).isEqualTo(5);
                assertThat(status.errorMessage()).isNull();
            });

        // Verify final position state after all transactions
        // 200 + 10 + 15 - 25 + 5 = 205 units
        // Complex cost calculation with dividend
        var finalPosition = componentClient.forEventSourcedEntity(positionId.toEntityId())
            .method(PositionEntity::getCurrentState)
            .invoke();

        assertThat(finalPosition.unitsHeld()).isEqualByComparingTo(BigDecimal.valueOf(205));
    }

    private void initializePosition(PositionId positionId, BigDecimal units, BigDecimal bookCost) {
        var openingBalance = new OpeningBalance(
            positionId.accountId(),
            positionId.instrumentId(),
            units,
            bookCost
        );

        var result = componentClient.forEventSourcedEntity(positionId.toEntityId())
            .method(PositionEntity::initializeFromOpeningBalance)
            .invoke(openingBalance);

        assertThat(result).isEqualTo(Done.getInstance());
    }

    private void verifyPositionState(PositionId positionId, BigDecimal expectedUnits, BigDecimal expectedBookCost) {
        var position = componentClient.forEventSourcedEntity(positionId.toEntityId())
            .method(PositionEntity::getCurrentState)
            .invoke();

        assertThat(position.unitsHeld()).isEqualByComparingTo(expectedUnits);
        assertThat(position.bookCost()).isEqualByComparingTo(expectedBookCost);
    }
}