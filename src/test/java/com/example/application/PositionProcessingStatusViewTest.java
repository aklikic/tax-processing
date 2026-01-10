package com.example.application;

import akka.javasdk.DependencyProvider;
import akka.javasdk.client.ComponentClient;
import akka.javasdk.http.HttpClient;
import akka.javasdk.testkit.TestKit;
import akka.javasdk.timer.TimerScheduler;
import com.example.domain.*;
import akka.javasdk.testkit.TestKitSupport;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.in;

/**
 * Integration test for PositionProcessingStatusView.
 * Tests the view updates correctly when PositionEntity emits events.
 */
public class PositionProcessingStatusViewTest {

    private TestKit testKit;
    private ComponentClient componentClient;

    private static final DependencyProvider mockDependencyProvider =
            new DependencyProvider() {
                @SuppressWarnings("unchecked")
                @Override
                public <T> T getDependency(Class<T> clazz) {
                    throw new IllegalArgumentException("Unknown dependency type: " + clazz);
                }
            };

    private TestKit.Settings testKitSettings() {
        return TestKit.Settings.DEFAULT
            .withEventSourcedEntityIncomingMessages(PositionEntity.class)
                .withDisabledComponents(Set.of())
                .withDependencyProvider(mockDependencyProvider);
    }

    @BeforeEach
    public void beforeAll() {
        try {
            this.testKit = (new TestKit(this.testKitSettings())).start();
            this.componentClient = this.testKit.getComponentClient();
        } catch (Exception var2) {
            throw var2;
        }
    }

    @AfterEach
    public void afterAll() {
        if (this.testKit != null) {
            this.testKit.stop();
        }

    }

    @Test
    public void testPositionInitializationTracking() {
        var positionId = "ACC000001-AAPL";
        var events = testKit.getEventSourcedEntityIncomingMessages(PositionEntity.class);

        // Publish position initialization event
        var initializedEvent = new PositionEvent.Initialized(
            new PositionId("ACC000001", "AAPL"),
            BigDecimal.valueOf(100.00),
            BigDecimal.valueOf(5000.00),
            BigDecimal.valueOf(50.00)
        );

        events.publish(initializedEvent, positionId);

        // Wait for view to be updated
        Awaitility.await()
            .atMost(10, TimeUnit.SECONDS)
            .untilAsserted(() -> {
                var result = componentClient.forView()
                    .method(PositionProcessingStatusView::getPosition)
                    .invoke(positionId);

                assertThat(result).isNotNull();
                assertThat(result.positionId()).isEqualTo(positionId);
                assertThat(result.accountId()).isEqualTo("ACC000001");
                assertThat(result.instrumentId()).isEqualTo("AAPL");
                assertThat(result.initialized()).isTrue();
                assertThat(result.transactionsProcessed()).isEqualTo(0);
                assertThat(result.currentUnitsHeld()).isEqualByComparingTo(BigDecimal.valueOf(100.00));
                assertThat(result.currentBookCost()).isEqualByComparingTo(BigDecimal.valueOf(5000.00));
                assertThat(result.totalGainLoss()).isEqualByComparingTo(BigDecimal.ZERO);
                assertThat(result.lastTransactionTime()).isEmpty();
            });
    }

    @Test
    public void testTransactionProcessingTracking() {
        var positionId = "ACC000002-MSFT";
        var events = testKit.getEventSourcedEntityIncomingMessages(PositionEntity.class);

        // First initialize the position
        var initializedEvent = new PositionEvent.Initialized(
            new PositionId("ACC000002", "MSFT"),
            BigDecimal.valueOf(200.00),
            BigDecimal.valueOf(10000.00),
            BigDecimal.valueOf(50.00)
        );
        events.publish(initializedEvent, positionId);

        // Process a buy transaction
        var buyTransaction = new Transaction(
            "TXN001",
            "ACC000002",
            "MSFT",
            TransactionType.BUY,
            Instant.now(),
            BigDecimal.valueOf(50.00),
            BigDecimal.valueOf(55.00),
            BigDecimal.valueOf(10.00)
        );

        var bookCostAdjustedEvent = new PositionEvent.BookCostAdjusted(
            new PositionId("ACC000002", "MSFT"),
            buyTransaction,
            BigDecimal.valueOf(250.00),
            BigDecimal.valueOf(12760.00),
            BigDecimal.valueOf(51.04)
        );
        events.publish(bookCostAdjustedEvent, positionId);

        // Wait for view to be updated
        Awaitility.await()
            .atMost(10, TimeUnit.SECONDS)
            .untilAsserted(() -> {
                var result = componentClient.forView()
                    .method(PositionProcessingStatusView::getPosition)
                    .invoke(positionId);

                assertThat(result).isNotNull();
                assertThat(result.initialized()).isTrue();
                assertThat(result.transactionsProcessed()).isEqualTo(1);
                assertThat(result.currentUnitsHeld()).isEqualByComparingTo(BigDecimal.valueOf(250.00));
                assertThat(result.currentBookCost()).isEqualByComparingTo(BigDecimal.valueOf(12760.00));
                assertThat(result.lastTransactionTime()).contains(buyTransaction.dateTime());
            });
    }

    @Test
    public void testGainLossTracking() {
        var positionId = "ACC000003-GOOGL";
        var events = testKit.getEventSourcedEntityIncomingMessages(PositionEntity.class);

        // Initialize position
        var initializedEvent = new PositionEvent.Initialized(
            new PositionId("ACC000003", "GOOGL"),
            BigDecimal.valueOf(100.00),
            BigDecimal.valueOf(15000.00),
            BigDecimal.valueOf(150.00)
        );
        events.publish(initializedEvent, positionId);

        // Process a sell transaction that generates gain/loss
        var sellTransaction = new Transaction(
            "TXN002",
            "ACC000003",
            "GOOGL",
            TransactionType.SELL,
            Instant.now(),
            BigDecimal.valueOf(50.00),
            BigDecimal.valueOf(160.00),
            BigDecimal.valueOf(10.00)
        );

        var gainLossEvent = new PositionEvent.GainLossIncurred(
            new PositionId("ACC000003", "GOOGL"),
            sellTransaction,
            BigDecimal.valueOf(490.00)  // $500 gain minus $10 fees
        );
        events.publish(gainLossEvent, positionId);

        // Wait for view to be updated
        Awaitility.await()
            .atMost(10, TimeUnit.SECONDS)
            .untilAsserted(() -> {
                var result = componentClient.forView()
                    .method(PositionProcessingStatusView::getPosition)
                    .invoke(positionId);

                assertThat(result).isNotNull();
                assertThat(result.totalGainLoss()).isEqualByComparingTo(BigDecimal.valueOf(490.00));
                assertThat(result.lastTransactionTime()).contains(sellTransaction.dateTime());
            });
    }

    @Test
    public void testTotalsQuery() {
        var events = testKit.getEventSourcedEntityIncomingMessages(PositionEntity.class);

        // Create positions for different accounts
        var accountPositions = new String[]{"ACC999-AAPL", "ACC999-MSFT", "ACC888-GOOGL"};

        for (var positionIdStr : accountPositions) {
            var parts = positionIdStr.split("-");
            var positionId = new PositionId(parts[0], parts[1]);
            var initializedEvent = new PositionEvent.Initialized(
                    positionId,
                BigDecimal.valueOf(100.00),
                BigDecimal.valueOf(5000.00),
                BigDecimal.valueOf(50.00)
            );
            events.publish(initializedEvent, positionIdStr);

            // Process a buy transaction
            var buyTransaction = new Transaction(
                    UUID.randomUUID().toString(),
                    positionId.accountId(),
                    positionId.instrumentId(),
                    TransactionType.BUY,
                    Instant.now(),
                    BigDecimal.valueOf(50.00),
                    BigDecimal.valueOf(55.00),
                    BigDecimal.valueOf(10.00)
            );

            var bookCostAdjustedEvent = new PositionEvent.BookCostAdjusted(
                    positionId,
                    buyTransaction,
                    BigDecimal.valueOf(250.00),
                    BigDecimal.valueOf(12760.00),
                    BigDecimal.valueOf(51.04)
            );
            events.publish(bookCostAdjustedEvent, positionIdStr);
        }

        // Query positions for ACC999
        Awaitility.await()
            .atMost(10, TimeUnit.SECONDS)
            .untilAsserted(() -> {
                var result = componentClient.forView()
                    .method(PositionProcessingStatusView::getAllPositionsCount)
                    .invoke();


                assertThat(result.totalCount()).isEqualTo(3);

                result = componentClient.forView()
                        .method(PositionProcessingStatusView::getPositionsByAccountCount)
                        .invoke("ACC999");


                assertThat(result.totalCount()).isEqualTo(2);

                result = componentClient.forView()
                        .method(PositionProcessingStatusView::getUnprocessedPositionsCount)
                        .invoke();


                assertThat(result.totalCount()).isEqualTo(0);

                result = componentClient.forView()
                        .method(PositionProcessingStatusView::getPositionsCountByTransactionCount)
                        .invoke(1);

                assertThat(result.totalCount()).isEqualTo(3);

            });
    }


}