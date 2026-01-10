package com.example.application;

import akka.javasdk.testkit.EventSourcedTestKit;
import com.example.domain.*;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for PositionEntity using EventSourcedTestKit.
 * Tests entity behavior in isolation without persistence infrastructure.
 */
public class PositionEntityTest {

    private static EventSourcedTestKit<Position, PositionEvent, PositionEntity> createTestKit(String batchId, String accountId, String instrument) {
        var testConfig = ConfigFactory.load();
        ProcessingConfig config = ProcessingConfig.fromConfig(testConfig);
        var positionId = new PositionId(accountId, instrument);
        var entityId = positionId.toEntityId(batchId);
        return EventSourcedTestKit.of(entityId, context -> new PositionEntity(context, config));
    }


    @Test
    public void shouldProcessBuyTransaction() {
        var testKit = createTestKit("batch","ACC002","MSFT");

        // Initialize position first with opening balance of 50 units @ $200/unit = $10,000
        var openingBalanceTransaction = new Transaction(
            "INIT001",
            "ACC002",
            "MSFT",
            TransactionType.BUY,
            Instant.now(),
            BigDecimal.valueOf(50),
            BigDecimal.valueOf(200), // $200 per unit
            BigDecimal.ZERO
        );

        testKit.method(PositionEntity::processTransaction).invoke(openingBalanceTransaction);

        // Process buy transaction
        var buyTransaction = new Transaction(
            "TX001",
            "ACC002",
            "MSFT",
            TransactionType.BUY,
            Instant.now(),
            BigDecimal.valueOf(25), // 25 units
            BigDecimal.valueOf(220), // $220 per unit
            BigDecimal.valueOf(10)   // $10 fees
        );

        var result = testKit.method(PositionEntity::processTransaction).invoke(buyTransaction);

        assertThat(result.isReply()).isTrue();

        // Verify events - should have 2 book cost adjusted events (initialization + buy)
        var allEvents = testKit.getAllEvents();
        assertThat(allEvents).hasSize(2);

        var bookCostEvent = (PositionEvent.BookCostAdjusted) allEvents.get(1);
        assertThat(bookCostEvent.unitsHeld()).isEqualByComparingTo(BigDecimal.valueOf(75)); // 50 + 25
        assertThat(bookCostEvent.bookCost()).isEqualByComparingTo(BigDecimal.valueOf(15510)); // 10000 + (25*220) + 10

        // Verify final state: 75 units, $15,510 cost
        var state = testKit.getState();
        assertThat(state.unitsHeld()).isEqualByComparingTo(BigDecimal.valueOf(75));
        assertThat(state.bookCost()).isEqualByComparingTo(BigDecimal.valueOf(15510));
        assertThat(state.centsPerUnit()).isEqualByComparingTo(BigDecimal.valueOf(206.8000)); // 15510 / 75
    }

    @Test
    public void shouldProcessSellTransactionWithGainLoss() {
        var testKit = createTestKit("batch","ACC003","GOOGL");

        // Initialize position first with 10 units @ $250/unit = $2,500
        var openingBalanceTransaction = new Transaction(
            "INIT002",
            "ACC003",
            "GOOGL",
            TransactionType.BUY,
            Instant.now(),
            BigDecimal.valueOf(10),
            BigDecimal.valueOf(250), // $250 per unit
            BigDecimal.ZERO
        );

        testKit.method(PositionEntity::processTransaction).invoke(openingBalanceTransaction);

        // Process sell transaction
        var sellTransaction = new Transaction(
            "TX002",
            "ACC003",
            "GOOGL",
            TransactionType.SELL,
            Instant.now(),
            BigDecimal.valueOf(5),   // 5 units
            BigDecimal.valueOf(280), // $280 per unit (gain expected)
            BigDecimal.valueOf(8)    // $8 fees
        );

        var result = testKit.method(PositionEntity::processTransaction).invoke(sellTransaction);

        assertThat(result.isReply()).isTrue();

        // Verify events - should have 1 buy + 2 sell events (gain/loss + book cost adjusted)
        var allEvents = testKit.getAllEvents();
        assertThat(allEvents).hasSize(3);

        var gainLossEvent = (PositionEvent.GainLossIncurred) allEvents.get(1);
        assertThat(gainLossEvent.gainLossAmount()).isEqualByComparingTo(BigDecimal.valueOf(142)); // (5*280-8) - (5*250) = 1392 - 1250 = 142

        var bookCostEvent = (PositionEvent.BookCostAdjusted) allEvents.get(2);
        assertThat(bookCostEvent.unitsHeld()).isEqualByComparingTo(BigDecimal.valueOf(5)); // 10 - 5
        assertThat(bookCostEvent.bookCost()).isEqualByComparingTo(BigDecimal.valueOf(1250)); // 2500 - 1250

        // Verify final state
        var state = testKit.getState();
        assertThat(state.unitsHeld()).isEqualByComparingTo(BigDecimal.valueOf(5));
        assertThat(state.bookCost()).isEqualByComparingTo(BigDecimal.valueOf(1250));
        assertThat(state.centsPerUnit()).isEqualByComparingTo(BigDecimal.valueOf(250));
    }

    @Test
    public void shouldProcessTransferInTransaction() {
        var testKit = createTestKit("batch","ACC004","TSLA");

        // Initialize position first with 20 units @ $200/unit = $4,000
        var openingBalanceTransaction = new Transaction(
            "INIT003",
            "ACC004",
            "TSLA",
            TransactionType.BUY,
            Instant.now(),
            BigDecimal.valueOf(20),
            BigDecimal.valueOf(200), // $200 per unit
            BigDecimal.ZERO
        );

        testKit.method(PositionEntity::processTransaction).invoke(openingBalanceTransaction);

        // Transfer in 10 more units
        var transferIn = new Transaction(
            "TX003",
            "ACC004",
            "TSLA",
            TransactionType.TRANSFER_IN,
            Instant.now(),
            BigDecimal.valueOf(10),
            BigDecimal.valueOf(250), // Transfer price
            BigDecimal.ZERO          // No fees on transfers
        );

        var result = testKit.method(PositionEntity::processTransaction).invoke(transferIn);

        assertThat(result.isReply()).isTrue();

        // Verify final state: 30 units, $6,500 cost
        var state = testKit.getState();
        assertThat(state.unitsHeld()).isEqualByComparingTo(BigDecimal.valueOf(30));
        assertThat(state.bookCost()).isEqualByComparingTo(BigDecimal.valueOf(6500)); // 4000 + (10*250)
        assertThat(state.centsPerUnit()).isEqualByComparingTo(BigDecimal.valueOf(216.6667)); // 6500 / 30
    }

    @Test
    public void shouldProcessCorporateActionTransaction() {
        var testKit = createTestKit("batch","ACC005","META");

        // Initialize position first with 100 units @ $200/unit = $20,000
        var openingBalanceTransaction = new Transaction(
            "INIT004",
            "ACC005",
            "META",
            TransactionType.BUY,
            Instant.now(),
            BigDecimal.valueOf(100),
            BigDecimal.valueOf(200), // $200 per unit
            BigDecimal.ZERO
        );

        testKit.method(PositionEntity::processTransaction).invoke(openingBalanceTransaction);

        // Corporate action - cash dividend
        var corporateAction = new Transaction(
            "TX004",
            "ACC005",
            "META",
            TransactionType.CORPORATE_ACTION,
            Instant.now(),
            BigDecimal.valueOf(100), // All units receive dividend
            BigDecimal.valueOf(5),   // $5 per unit dividend
            BigDecimal.ZERO
        );

        var result = testKit.method(PositionEntity::processTransaction).invoke(corporateAction);

        assertThat(result.isReply()).isTrue();

        // Verify final state: same units, reduced book cost
        var state = testKit.getState();
        assertThat(state.unitsHeld()).isEqualByComparingTo(BigDecimal.valueOf(100)); // Units unchanged
        assertThat(state.bookCost()).isEqualByComparingTo(BigDecimal.valueOf(19500)); // 20000 - (100*5)
        assertThat(state.centsPerUnit()).isEqualByComparingTo(BigDecimal.valueOf(195)); // 19500 / 100
    }


}