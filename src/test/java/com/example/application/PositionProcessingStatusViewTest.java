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
import java.util.List;
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
    public void testTransactionProcessingTracking() {
        var batchId = "batch";
        var position =  new PositionId("ACC000002", "MSFT");
        var positionId = position.toEntityId(batchId);
        var events = testKit.getEventSourcedEntityIncomingMessages(PositionEntity.class);

        // Process a buy transaction
        var buyTransaction = new Transaction(
            "TXN001",
            position.accountId(),
            position.instrumentId(),
            TransactionType.BUY,
            Instant.now(),
            BigDecimal.valueOf(50.00),
            BigDecimal.valueOf(55.00),
            BigDecimal.valueOf(10.00)
        );

        var bookCostAdjustedEvent = new PositionEvent.BookCostAdjusted(
            position,
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
                assertThat(result.transactionsProcessed()).isEqualTo(1);
                assertThat(result.currentUnitsHeld()).isEqualByComparingTo(BigDecimal.valueOf(250.00));
                assertThat(result.currentBookCost()).isEqualByComparingTo(BigDecimal.valueOf(12760.00));
                assertThat(result.lastTransactionTime()).contains(buyTransaction.dateTime());
            });
    }

    @Test
    public void testGainLossTracking() {
        var batchId = "batch";
        var position =  new PositionId("ACC000003", "GOOGL");
        var positionId = position.toEntityId(batchId);
        var events = testKit.getEventSourcedEntityIncomingMessages(PositionEntity.class);

        // Process a sell transaction that generates gain/loss
        var sellTransaction = new Transaction(
            "TXN002",
            position.accountId(),
            position.instrumentId(),
            TransactionType.SELL,
            Instant.now(),
            BigDecimal.valueOf(50.00),
            BigDecimal.valueOf(160.00),
            BigDecimal.valueOf(10.00)
        );

        var gainLossEvent = new PositionEvent.GainLossIncurred(
            position,
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
        var batchId = "batch";
        var events = testKit.getEventSourcedEntityIncomingMessages(PositionEntity.class);

        // Create positions for different accounts
        var accountPositionIds = List.of(new PositionId("ACC999","AAPL"), new PositionId("ACC999","MSFT"), new PositionId("ACC888","GOOGL"));

        for (var positionId : accountPositionIds) {
            var positionIdStr = positionId.toEntityId(batchId);

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