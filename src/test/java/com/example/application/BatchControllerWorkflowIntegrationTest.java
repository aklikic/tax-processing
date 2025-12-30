package com.example.application;

import akka.Done;
import akka.javasdk.DependencyProvider;
import akka.javasdk.testkit.TestKit;
import akka.javasdk.testkit.TestKitSupport;
import com.example.domain.*;
import com.example.infrastructure.MockTaxDataRepository;
import com.typesafe.config.ConfigFactory;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for OpeningBalanceBatch workflow.
 * Tests the complete main workflow execution including window processing,
 * position initialization, sub-workflow coordination, and callback handling.
 */
public class BatchControllerWorkflowIntegrationTest extends TestKitSupport {

    private final MockTaxDataRepository mockRepository = new MockTaxDataRepository();

    @Override
    protected TestKit.Settings testKitSettings() {
        // Create test configuration with smaller batch sizes for testing
        var testConfig = ConfigFactory.parseString("""
            tax-processing {
                positions-per-window = 4
                position-init-batch-size = 2
                positions-per-batch = 2
                transaction-window-size = 5
                position-idempotency-cache-size = 100
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

    @BeforeEach
    public void setUp() {
        mockRepository.clearError();
        mockRepository.clearData();
    }

    @Test
    public void shouldProcessSingleWindowSuccessfully() {
        // Setup test data - 3 opening balances (less than window size of 4)
        // Use unique account IDs to avoid conflicts with other tests
        var testId = System.currentTimeMillis();
        var openingBalances = List.of(
            new OpeningBalance("ACC" + testId + "01", "AAPL", BigDecimal.valueOf(100), BigDecimal.valueOf(15000)),
            new OpeningBalance("ACC" + testId + "02", "MSFT", BigDecimal.valueOf(50), BigDecimal.valueOf(10000)),
            new OpeningBalance("ACC" + testId + "03", "GOOGL", BigDecimal.valueOf(25), BigDecimal.valueOf(7500))
        );

        var transactions = List.of(
            new Transaction("TX001", "ACC" + testId + "01", "AAPL", TransactionType.BUY,
                Instant.now(), BigDecimal.valueOf(25), BigDecimal.valueOf(155), BigDecimal.valueOf(5)),
            new Transaction("TX002", "ACC" + testId + "02", "MSFT", TransactionType.BUY,
                Instant.now(), BigDecimal.valueOf(20), BigDecimal.valueOf(205), BigDecimal.valueOf(8)),
            new Transaction("TX003", "ACC" + testId + "03", "GOOGL", TransactionType.SELL,
                Instant.now(), BigDecimal.valueOf(10), BigDecimal.valueOf(305), BigDecimal.valueOf(3))
        );

        mockRepository.setupOpeningBalances("2023", List.of(openingBalances));
        mockRepository.setupTransactions("2023", List.of(transactions, List.of())); // One window + empty

        var batchId = "main-batch-001-" + testId;

        var startCommand = new BatchControllerWorkflow.StartBatchCommand(batchId, "2023");

        var startResult = componentClient.forWorkflow(batchId)
            .method(BatchControllerWorkflow::start)
            .invoke(startCommand);

        assertThat(startResult).isEqualTo(Done.getInstance());

        // Wait for workflow completion
        Awaitility.await()
            .atMost(60, TimeUnit.SECONDS)
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted(() -> {
                var status = componentClient.forWorkflow(batchId)
                    .method(BatchControllerWorkflow::getStatus)
                    .invoke();

                assertThat(status.status()).isEqualTo(
                   BatchControllerState.ProcessingStatus.COMPLETED
                );
//                System.out.println(status);
                assertThat(status.totalPositions()).isEqualTo(3);
                assertThat(status.totalWindows()).isEqualTo(1);
                assertThat(status.errorMessage()).isNull();
            });

        // Verify all positions were processed
        verifyPositionExists(new PositionId("ACC" + testId + "01", "AAPL"));
        verifyPositionExists(new PositionId("ACC" + testId + "02", "MSFT"));
        verifyPositionExists(new PositionId("ACC" + testId + "03", "GOOGL"));
    }

    @Test
    public void shouldProcessMultipleWindowsSequentially() {
        // Setup test data - 7 opening balances across 2 windows (window size = 4)
        var testId = System.currentTimeMillis() + 1000;  // Add offset to avoid collision
        var window1Balances = List.of(
            new OpeningBalance("ACC" + testId + "01", "AAPL", BigDecimal.valueOf(100), BigDecimal.valueOf(15000)),
            new OpeningBalance("ACC" + testId + "02", "MSFT", BigDecimal.valueOf(50), BigDecimal.valueOf(10000)),
            new OpeningBalance("ACC" + testId + "03", "GOOGL", BigDecimal.valueOf(25), BigDecimal.valueOf(7500)),
            new OpeningBalance("ACC" + testId + "04", "TSLA", BigDecimal.valueOf(75), BigDecimal.valueOf(22500))
        );

        var window2Balances = List.of(
            new OpeningBalance("ACC" + testId + "05", "NVDA", BigDecimal.valueOf(30), BigDecimal.valueOf(12000)),
            new OpeningBalance("ACC" + testId + "06", "META", BigDecimal.valueOf(40), BigDecimal.valueOf(8000)),
            new OpeningBalance("ACC" + testId + "07", "AMZN", BigDecimal.valueOf(20), BigDecimal.valueOf(6000))
        );

        var transactions = List.of(
            new Transaction("TX" + testId + "01", "ACC" + testId + "01", "AAPL", TransactionType.BUY,
                Instant.now(), BigDecimal.valueOf(10), BigDecimal.valueOf(155), BigDecimal.valueOf(2)),
            new Transaction("TX" + testId + "02", "ACC" + testId + "05", "NVDA", TransactionType.SELL,
                Instant.now(), BigDecimal.valueOf(5), BigDecimal.valueOf(405), BigDecimal.valueOf(3))
        );

        // Setup opening balances for multiple windows
        mockRepository.setupOpeningBalances("2023", List.of(window1Balances, window2Balances, List.of()));
        mockRepository.setupTransactions("2023", List.of(transactions, List.of()));

        var batchId = "main-batch-multi-" + testId;
        var workflowId = batchId;

        var startCommand = new BatchControllerWorkflow.StartBatchCommand(batchId, "2023");

        componentClient.forWorkflow(workflowId)
            .method(BatchControllerWorkflow::start)
            .invoke(startCommand);

        // Wait for all windows to be processed
        Awaitility.await()
            .atMost(90, TimeUnit.SECONDS)
            .pollInterval(Duration.ofSeconds(1))
            .untilAsserted(() -> {
                var status = componentClient.forWorkflow(workflowId)
                    .method(BatchControllerWorkflow::getStatus)
                    .invoke();

                assertThat(status.status()).isEqualTo(
                    BatchControllerState.ProcessingStatus.COMPLETED
                );
                assertThat(status.totalPositions()).isEqualTo(7);
                assertThat(status.totalWindows()).isEqualTo(2);
            });

        // Verify all positions from both windows were processed
        verifyPositionExists(new PositionId("ACC" + testId + "01", "AAPL"));
        verifyPositionExists(new PositionId("ACC" + testId + "02", "MSFT"));
        verifyPositionExists(new PositionId("ACC" + testId + "03", "GOOGL"));
        verifyPositionExists(new PositionId("ACC" + testId + "04", "TSLA"));
        verifyPositionExists(new PositionId("ACC" + testId + "05", "NVDA"));
        verifyPositionExists(new PositionId("ACC" + testId + "06", "META"));
        verifyPositionExists(new PositionId("ACC" + testId + "07", "AMZN"));
    }


    @Test
    public void shouldHandleEmptyOpeningBalances() {
        // Setup empty opening balances
        mockRepository.setupOpeningBalances("2023", List.of(List.of()));

        var testId = System.currentTimeMillis() + 6000;  // Add offset to avoid collision
        var batchId = "main-batch-empty-" + testId;
        var workflowId = batchId;

        var startCommand = new BatchControllerWorkflow.StartBatchCommand(batchId, "2023");

        componentClient.forWorkflow(workflowId)
            .method(BatchControllerWorkflow::start)
            .invoke(startCommand);

        // Workflow should complete quickly with no data
        Awaitility.await()
            .atMost(20, TimeUnit.SECONDS)
            .untilAsserted(() -> {
                var status = componentClient.forWorkflow(workflowId)
                    .method(BatchControllerWorkflow::getStatus)
                    .invoke();

                assertThat(status.status()).isEqualTo(
                        BatchControllerState.ProcessingStatus.COMPLETED
                );
                assertThat(status.totalPositions()).isEqualTo(0);
                assertThat(status.totalWindows()).isEqualTo(0);
            });
    }

    @Test
    public void shouldHandleRepositoryErrors() {
        var testId = System.currentTimeMillis() + 7000;  // Add offset to avoid collision
        var batchId = "main-batch-error-" + testId;
        var workflowId = batchId;

        var openingBalances = List.of(
                new OpeningBalance("ACC" + testId + "01", "AAPL", BigDecimal.valueOf(100), BigDecimal.valueOf(15000)),
                new OpeningBalance("ACC" + testId + "02", "MSFT", BigDecimal.valueOf(50), BigDecimal.valueOf(10000)),
                new OpeningBalance("ACC" + testId + "03", "GOOGL", BigDecimal.valueOf(25), BigDecimal.valueOf(7500))
        );

        var transactions = List.of(
                new Transaction("TX001", "ACC" + testId + "01", "AAPL", TransactionType.BUY,
                        Instant.now(), BigDecimal.valueOf(25), BigDecimal.valueOf(155), BigDecimal.valueOf(5)),
                new Transaction("TX002", "ACC" + testId + "02", "MSFT", TransactionType.BUY,
                        Instant.now(), BigDecimal.valueOf(20), BigDecimal.valueOf(205), BigDecimal.valueOf(8)),
                new Transaction("TX003", "ACC" + testId + "03", "GOOGL", TransactionType.SELL,
                        Instant.now(), BigDecimal.valueOf(10), BigDecimal.valueOf(305), BigDecimal.valueOf(3))
        );

        mockRepository.setupOpeningBalances("2023", List.of(openingBalances));
        mockRepository.setupTransactions("2023", List.of(transactions, List.of())); // One window + empty


        // Setup repository to fail during count
        mockRepository.setupError("Database connection timeout");

        var startCommand = new BatchControllerWorkflow.StartBatchCommand(batchId, "2023");

        componentClient.forWorkflow(workflowId)
            .method(BatchControllerWorkflow::start)
            .invoke(startCommand);

        // Workflow should fail and transition to error handling
        Awaitility.await()
            .atMost(30, TimeUnit.SECONDS)
            .untilAsserted(() -> {
                var status = componentClient.forWorkflow(workflowId)
                    .method(BatchControllerWorkflow::getStatus)
                    .invoke();
                assertThat(status.status()).isEqualTo(
                        BatchControllerState.ProcessingStatus.COMPLETED
                );
                var failedPositions = status.totalPositions() - status.completedPositions();
                assertThat(failedPositions).isGreaterThan(0l);
            });
    }


    @Test
    public void shouldProcessLargerBatchWithMultipleMicrobatches() {
        // Setup data that will create multiple microbatches (microbatch size = 2)
        var testId = System.currentTimeMillis() + 5000;  // Add offset to avoid collision
        var openingBalances = List.of(
            new OpeningBalance("ACC" + testId + "01", "AAPL", BigDecimal.valueOf(100), BigDecimal.valueOf(15000)),
            new OpeningBalance("ACC" + testId + "02", "MSFT", BigDecimal.valueOf(50), BigDecimal.valueOf(10000)),
            new OpeningBalance("ACC" + testId + "03", "GOOGL", BigDecimal.valueOf(25), BigDecimal.valueOf(7500)),
            new OpeningBalance("ACC" + testId + "04", "TSLA", BigDecimal.valueOf(75), BigDecimal.valueOf(22500)),
            new OpeningBalance("ACC" + testId + "05", "NVDA", BigDecimal.valueOf(30), BigDecimal.valueOf(12000))
        );

        // This will create 3 microbatches: [ACC001,ACC002], [ACC003,ACC004], [ACC005]
        mockRepository.setupOpeningBalances("2023", List.of(openingBalances.subList(0, 4), openingBalances.subList(4, 5), List.of()));
        mockRepository.setupTransactions("2023", List.of(List.of())); // No transactions for simplicity

        var batchId = "main-batch-microbatch-" + testId;
        var workflowId = batchId;

        var startCommand = new BatchControllerWorkflow.StartBatchCommand(batchId, "2023");

        componentClient.forWorkflow(workflowId)
            .method(BatchControllerWorkflow::start)
            .invoke(startCommand);

        // Wait for completion
        Awaitility.await()
            .atMost(90, TimeUnit.SECONDS)
            .pollInterval(Duration.ofSeconds(1))
            .untilAsserted(() -> {
                var status = componentClient.forWorkflow(workflowId)
                    .method(BatchControllerWorkflow::getStatus)
                    .invoke();

                assertThat(status.status()).isEqualTo(
                        BatchControllerState.ProcessingStatus.COMPLETED
                );
                assertThat(status.totalPositions()).isEqualTo(5);
                assertThat(status.totalWindows()).isEqualTo(2);
            });

        // Verify all positions were processed
        verifyPositionExists(new PositionId("ACC" + testId + "01", "AAPL"));
        verifyPositionExists(new PositionId("ACC" + testId + "02", "MSFT"));
        verifyPositionExists(new PositionId("ACC" + testId + "03", "GOOGL"));
        verifyPositionExists(new PositionId("ACC" + testId + "04", "TSLA"));
        verifyPositionExists(new PositionId("ACC" + testId + "05", "NVDA"));

        Awaitility.await()
                .atMost(90, TimeUnit.SECONDS)
                .pollInterval(Duration.ofSeconds(1))
                .untilAsserted(() -> {
                    for(OpeningBalance ob : openingBalances) {
                        var res = componentClient.forView()
                                .method(PositionProcessingStatusView::getPosition)
                                .invoke(ob.positionId().toEntityId());
                        assertThat(res.initialized()).isTrue();
                        assertThat(res.transactionsProcessed()).isEqualTo(0);
                    }

                });

    }

    private void verifyPositionExists(PositionId positionId) {
        var position = componentClient.forEventSourcedEntity(positionId.toEntityId())
            .method(PositionEntity::getCurrentState)
            .invoke();

        System.out.println("Position " + positionId + ": " + position);
        assertThat(position).isNotNull();
        assertThat(position.unitsHeld()).describedAs("Position %s should have units > 0, but has %s", positionId, position.unitsHeld())
            .isGreaterThan(BigDecimal.ZERO);
    }
}