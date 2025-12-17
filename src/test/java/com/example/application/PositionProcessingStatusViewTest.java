package com.example.application;

import akka.javasdk.testkit.TestKit;
import com.example.domain.*;
import akka.javasdk.testkit.TestKitSupport;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for PositionProcessingStatusView.
 * Tests the view updates correctly when PositionEntity emits events.
 */
public class PositionProcessingStatusViewTest extends TestKitSupport {

    @Override
    protected TestKit.Settings testKitSettings() {
        return TestKit.Settings.DEFAULT
            .withEventSourcedEntityIncomingMessages(PositionEntity.class);
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
    public void testProcessingSummary() {
        var events = testKit.getEventSourcedEntityIncomingMessages(PositionEntity.class);

        // Create multiple positions for summary testing
        var positions = new String[]{"ACC001-AAPL", "ACC001-MSFT", "ACC002-GOOGL"};

        for (int i = 0; i < positions.length; i++) {
            var positionId = positions[i];
            var parts = positionId.split("-");

            // Initialize each position
            var initializedEvent = new PositionEvent.Initialized(
                new PositionId(parts[0], parts[1]),
                BigDecimal.valueOf(100.00),
                BigDecimal.valueOf(5000.00),
                BigDecimal.valueOf(50.00)
            );
            events.publish(initializedEvent, positionId);

            // Add some transaction processing for variety
            if (i > 0) {
                var transaction = new Transaction(
                    "TXN00" + i,
                    parts[0],
                    parts[1],
                    TransactionType.BUY,
                    Instant.now().minus(i, ChronoUnit.HOURS),
                    BigDecimal.valueOf(10.00),
                    BigDecimal.valueOf(50.00),
                    BigDecimal.valueOf(5.00)
                );

                var bookCostEvent = new PositionEvent.BookCostAdjusted(
                    new PositionId(parts[0], parts[1]),
                    transaction,
                    BigDecimal.valueOf(110.00),
                    BigDecimal.valueOf(5505.00),
                    BigDecimal.valueOf(50.05)
                );
                events.publish(bookCostEvent, positionId);
            }
        }

        // Wait for all positions to be processed and check counts
        Awaitility.await()
            .atMost(15, TimeUnit.SECONDS)
            .untilAsserted(() -> {
                var allPositions = componentClient.forView()
                    .method(PositionProcessingStatusView::getAllPositionsForSummary)
                    .invoke();

                assertThat(allPositions.positions()).hasSize(3);
                assertThat(allPositions.positions().stream().mapToInt(p -> p.initialized() ? 1 : 0).sum()).isEqualTo(3);
                assertThat(allPositions.positions().stream().mapToInt(p -> p.transactionsProcessed() > 0 ? 1 : 0).sum()).isEqualTo(2); // Only 2 have processed transactions
                assertThat(allPositions.positions().stream().mapToInt(PositionProcessingStatusView.PositionStatusEntry::transactionsProcessed).sum()).isEqualTo(2);
            });
    }

    @Test
    public void testQueryByAccount() {
        var events = testKit.getEventSourcedEntityIncomingMessages(PositionEntity.class);

        // Create positions for different accounts
        var accountPositions = new String[]{"ACC999-AAPL", "ACC999-MSFT", "ACC888-GOOGL"};

        for (var positionId : accountPositions) {
            var parts = positionId.split("-");
            var initializedEvent = new PositionEvent.Initialized(
                new PositionId(parts[0], parts[1]),
                BigDecimal.valueOf(100.00),
                BigDecimal.valueOf(5000.00),
                BigDecimal.valueOf(50.00)
            );
            events.publish(initializedEvent, positionId);
        }

        // Query positions for ACC999
        Awaitility.await()
            .atMost(10, TimeUnit.SECONDS)
            .untilAsserted(() -> {
                var result = componentClient.forView()
                    .method(PositionProcessingStatusView::getPositionsByAccount)
                    .invoke("ACC999");

                assertThat(result.positions()).hasSize(2);
                assertThat(result.positions()).allMatch(p -> p.accountId().equals("ACC999"));
                var instruments = result.positions().stream()
                    .map(PositionProcessingStatusView.PositionStatusEntry::instrumentId)
                    .sorted()
                    .toList();
                assertThat(instruments).containsExactly("AAPL", "MSFT");
            });
    }
}