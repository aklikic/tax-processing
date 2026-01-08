package com.example.application;

import akka.Done;
import akka.javasdk.DependencyProvider;
import akka.javasdk.testkit.TestKit;
import akka.javasdk.testkit.TestKitSupport;
import com.example.domain.PositionBatchControllerState;
import com.example.domain.PositionId;
import com.example.domain.ProcessingConfig;
import com.example.infrastructure.DatabaseConfiguration;
import com.example.infrastructure.PostgreSQLTaxDataRepository;
import com.example.infrastructure.PostgreSQLTestDataHelper;
import com.typesafe.config.ConfigFactory;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PostgreSQL integration test for BatchControllerWorkflow using Testcontainers.
 * Tests the complete workflow execution against a real PostgreSQL database.
 *
 */
@Testcontainers
@DisabledIfEnvironmentVariable(named = "docker", matches = "false", disabledReason = "Docker disabled")
public class PositionBatchControllerWorkflowPostgreSQLIntegrationTest extends TestKitSupport {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine")
            .withDatabaseName("TaxProcessing")
            .withUsername("testuser")
            .withPassword("testpassword");

    private PostgreSQLTaxDataRepository repository;
    private PostgreSQLTestDataHelper testDataHelper;

    @Override
    protected TestKit.Settings testKitSettings() {

        // Create test configuration with smaller batch sizes for testing
        var testConfig = ConfigFactory.parseString(String.format("""
            tax-processing {
               position-number-per-window = 2
               position-max-parallel-windows = 2
               position-idempotency-cache-size = 10
               position-max-completed-windows-to-keep-in-state = 10
               transactions-batch-limit = 1
               transactions-batch-parallelism = 25
                                
                database {
                    enable = true
                    host = "%s"
                    port = %d
                    database = "%s"
                    username = "%s"
                    password = "%s"
                    monitoring-delay = 0
                    ssl-enabled = false
                    pool {
                        initial-size = 2
                        max-size = 10
                        max-idle-time = "5 minutes"
                        max-lifetime = "30 minutes"
                        max-acquire-time = "30 seconds"
                        max-create-connection-time = "30 seconds"
                    
                    }
                }
            }
            """,
            postgres.getHost(),
            postgres.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT),
            postgres.getDatabaseName(),
            postgres.getUsername(),
            postgres.getPassword()
        ));

        var processingConfig = ProcessingConfig.fromConfig(testConfig);
        var connectionFactory = DatabaseConfiguration.createConnectionFactory(testConfig);
        repository = new PostgreSQLTaxDataRepository(connectionFactory);

        testDataHelper = new PostgreSQLTestDataHelper(
            postgres.getJdbcUrl(),
            postgres.getUsername(),
            postgres.getPassword()
        );

        var dependencyProvider = new DependencyProvider() {
            @SuppressWarnings("unchecked")
            @Override
            public <T> T getDependency(Class<T> clazz) {
                if (clazz.equals(TaxDataRepository.class)) {
                    return (T) repository;
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
    public void setUp() throws Exception {
        testDataHelper.createTables();
    }

    @Test
    public void shouldProcessSmallBatchWithPostgreSQL() throws Exception {
        var testId = System.currentTimeMillis();
        var taxYear = "2023";

        // Setup test data using helper
        testDataHelper.clearTestData(taxYear);
        testDataHelper.insertSmallTestDataset(taxYear, testId);

        var batchId = "postgres-batch-" + testId;
        var startCommand = new PositionBatchControllerWorkflow.StartBatchCommand(batchId, taxYear);

        var startResult = componentClient.forWorkflow(batchId)
            .method(PositionBatchControllerWorkflow::start)
            .invoke(startCommand);

        assertThat(startResult).isEqualTo(Done.getInstance());

        // Wait for workflow completion with generous timeout for database operations
        Awaitility.await()
            .atMost(2, TimeUnit.MINUTES)
            .pollInterval(Duration.ofSeconds(2))
            .untilAsserted(() -> {
                var status = componentClient.forWorkflow(batchId)
                    .method(PositionBatchControllerWorkflow::getStatus)
                    .invoke();

                System.out.println("Status: " + status);

                assertThat(status.status()).isEqualTo(
                    PositionBatchControllerState.ProcessingStatus.COMPLETED
                );
                assertThat(status.totalPositions()).isEqualTo(3);
                assertThat(status.totalWindows()).isEqualTo(2); // 3 positions with window size 2 = 2 window
                assertThat(status.errorMessage()).isNull();
            });

        // Verify all positions were processed by checking entities
        verifyPositionExists(new PositionId("ACC" + testId + "01", "AAPL"));
        verifyPositionExists(new PositionId("ACC" + testId + "02", "MSFT"));
        verifyPositionExists(new PositionId("ACC" + testId + "03", "GOOGL"));
    }

    @Test
    public void shouldHandleEmptyDatabaseGracefully() throws Exception {
        var testId = System.currentTimeMillis() + 1000;
        var taxYear = "2024"; // Different year with no data
        var batchId = "empty-postgres-batch-" + testId;

        // Ensure no data exists for this tax year
        testDataHelper.clearTestData(taxYear);

        var startCommand = new PositionBatchControllerWorkflow.StartBatchCommand(batchId, taxYear);

        componentClient.forWorkflow(batchId)
            .method(PositionBatchControllerWorkflow::start)
            .invoke(startCommand);

        // Workflow should complete quickly with no data
        Awaitility.await()
            .atMost(30, TimeUnit.SECONDS)
            .pollInterval(Duration.ofSeconds(1))
            .untilAsserted(() -> {
                var status = componentClient.forWorkflow(batchId)
                    .method(PositionBatchControllerWorkflow::getStatus)
                    .invoke();

                assertThat(status.status()).isEqualTo(
                        PositionBatchControllerState.ProcessingStatus.COMPLETED
                );
                assertThat(status.totalPositions()).isEqualTo(0);
                assertThat(status.totalWindows()).isEqualTo(0);
                assertThat(status.errorMessage()).isNull();
            });
    }

    @Test
    public void shouldProcessMultipleWindowsWithPostgreSQL() throws Exception {
        var testId = System.currentTimeMillis() + 2000;
        var taxYear = "2024";

        // Setup test data for multiple windows using helper
        testDataHelper.clearTestData(taxYear);
        testDataHelper.insertMultiWindowTestDataset(taxYear, testId);

        var batchId = "multi-window-postgres-" + testId;
        var startCommand = new PositionBatchControllerWorkflow.StartBatchCommand(batchId, taxYear);

        componentClient.forWorkflow(batchId)
            .method(PositionBatchControllerWorkflow::start)
            .invoke(startCommand);

        // Wait for all windows to be processed
        Awaitility.await()
            .atMost(3, TimeUnit.MINUTES)
            .pollInterval(Duration.ofSeconds(3))
            .untilAsserted(() -> {
                var status = componentClient.forWorkflow(batchId)
                    .method(PositionBatchControllerWorkflow::getStatus)
                    .invoke();

                System.out.println("Multi-window Status: " + status);

                assertThat(status.status()).isEqualTo(
                        PositionBatchControllerState.ProcessingStatus.COMPLETED
                );
                assertThat(status.totalPositions()).isEqualTo(5);
                assertThat(status.totalWindows()).isEqualTo(3);// 5 transactions with window size 2 = 3 windows
                assertThat(status.errorMessage()).isNull();
            });

        // Verify all positions from both windows were processed
        verifyPositionExists(new PositionId("ACC" + testId + "01", "AAPL"));
        verifyPositionExists(new PositionId("ACC" + testId + "05", "NVDA"));
    }

    private void verifyPositionExists(PositionId positionId) {
        var position = componentClient.forEventSourcedEntity(positionId.toEntityId())
            .method(PositionEntity::getCurrentState)
            .invoke();

        System.out.println("PostgreSQL Position " + positionId + ": " + position);
        assertThat(position).isNotNull();
        assertThat(position.unitsHeld()).describedAs("Position %s should have units > 0, but has %s", positionId, position.unitsHeld())
            .isGreaterThan(BigDecimal.ZERO);
    }
}