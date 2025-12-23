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
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Simple integration test for OpeningBalanceBatch workflow to verify basic functionality.
 */
public class BatchControllerWorkflowSimpleTest extends TestKitSupport {

    private final MockTaxDataRepository mockRepository = new MockTaxDataRepository();

    @Override
    protected TestKit.Settings testKitSettings() {
        // Create test configuration with smaller batch sizes for testing
        var testConfig = ConfigFactory.parseString("""
            tax-processing {
                positions-per-window = 4
                position-init-batch-size = 2
                transaction-microbatch-size = 2
                transaction-window-size = 5
                max-parallel-sub-workflows = 6
                completion-window = 2
                emergency-threshold = 2
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
    public void shouldHandleEmptyOpeningBalances() {
        // Setup empty opening balances
        mockRepository.setupOpeningBalances("2023", List.of(List.of()));

        var testId = System.currentTimeMillis();
        var batchId = "empty-batch-" + testId;

        var startCommand = new BatchControllerWorkflow.StartBatchCommand(batchId, "2023");

        componentClient.forWorkflow(batchId)
            .method(BatchControllerWorkflow::start)
            .invoke(startCommand);

        // Workflow should complete quickly with no data
        Awaitility.await()
            .atMost(20, TimeUnit.SECONDS)
            .untilAsserted(() -> {
                var status = componentClient.forWorkflow(batchId)
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
    public void shouldProcessSinglePositionSuccessfully() {
        // Setup test data - 1 opening balance
        var testId = System.currentTimeMillis();
        var openingBalances = List.of(
            new OpeningBalance("ACC" + testId, "AAPL", BigDecimal.valueOf(100), BigDecimal.valueOf(15000))
        );

        // Use non-windowed mode: directly add opening balances to repository
        mockRepository.clearData();
        openingBalances.forEach(mockRepository::addOpeningBalance);
        // No transactions needed for this test

        var batchId = "single-batch-" + testId;

        var startCommand = new BatchControllerWorkflow.StartBatchCommand(batchId, "2023");

        var startResult = componentClient.forWorkflow(batchId)
            .method(BatchControllerWorkflow::start)
            .invoke(startCommand);

        assertThat(startResult).isEqualTo(Done.getInstance());

        // Wait for workflow completion
        Awaitility.await()
            .atMost(30, TimeUnit.SECONDS)
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted(() -> {
                var status = componentClient.forWorkflow(batchId)
                    .method(BatchControllerWorkflow::getStatus)
                    .invoke();

                assertThat(status.status()).isEqualTo(
                    BatchControllerState.ProcessingStatus.COMPLETED
                );
//                System.out.println(status);
                assertThat(status.totalPositions()).isEqualTo(1);
                assertThat(status.totalWindows()).isEqualTo(1);
                assertThat(status.errorMessage()).isNull();
            });

        // Verify position was processed
        var position = componentClient.forEventSourcedEntity(new PositionId("ACC" + testId, "AAPL").toEntityId())
            .method(PositionEntity::getCurrentState)
            .invoke();

        assertThat(position).isNotNull();
        assertThat(position.unitsHeld()).isEqualTo(BigDecimal.valueOf(100));
        assertThat(position.bookCost()).isEqualTo(BigDecimal.valueOf(15000));
    }
}