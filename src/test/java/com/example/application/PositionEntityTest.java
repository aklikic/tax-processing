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

    private static EventSourcedTestKit<Position, PositionEvent, PositionEntity> createTestKit(String entityId) {
        var testConfig = ConfigFactory.parseString("""
            tax-processing {
                positions-per-window = 5000
                position-init-batch-size = 500
                positions-per-batch = 111
                transaction-window-size = 320
                position-idempotency-cache-size = 1000
                max-parallel-windows = 3
            }
            """);
        ProcessingConfig config = ProcessingConfig.fromConfig(testConfig);
        return EventSourcedTestKit.of(entityId, context -> new PositionEntity(context, config));
    }

    @Test
    public void shouldInitializePositionFromOpeningBalance() {
        var testKit = createTestKit("ACC001-AAPL");

        var openingBalance = new OpeningBalance(
            "ACC001",
            "AAPL",
            BigDecimal.valueOf(100),
            BigDecimal.valueOf(15000)
        );

        var result = testKit.method(PositionEntity::initializeFromOpeningBalance).invoke(openingBalance);

        assertThat(result.isReply()).isTrue();

        // Verify event was persisted
        var events = testKit.getAllEvents();
        assertThat(events).hasSize(1);

        var initEvent = (PositionEvent.Initialized) events.get(0);
        assertThat(initEvent.positionId().accountId()).isEqualTo("ACC001");
        assertThat(initEvent.positionId().instrumentId()).isEqualTo("AAPL");
        assertThat(initEvent.initialUnits()).isEqualByComparingTo(BigDecimal.valueOf(100));
        assertThat(initEvent.initialBookCost()).isEqualByComparingTo(BigDecimal.valueOf(15000));
        assertThat(initEvent.initialCentsPerUnit()).isEqualByComparingTo(BigDecimal.valueOf(150));

        // Verify final state
        var state = testKit.getState();
        assertThat(state.accountId()).isEqualTo("ACC001");
        assertThat(state.instrumentId()).isEqualTo("AAPL");
        assertThat(state.unitsHeld()).isEqualByComparingTo(BigDecimal.valueOf(100));
        assertThat(state.bookCost()).isEqualByComparingTo(BigDecimal.valueOf(15000));
        assertThat(state.centsPerUnit()).isEqualByComparingTo(BigDecimal.valueOf(150));
    }

    @Test
    public void shouldProcessBuyTransaction() {
        var testKit = createTestKit("ACC002-MSFT");

        // Initialize position first
        var openingBalance = new OpeningBalance(
            "ACC002",
            "MSFT",
            BigDecimal.valueOf(50),
            BigDecimal.valueOf(10000)
        );

        testKit.method(PositionEntity::initializeFromOpeningBalance).invoke(openingBalance);

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
        var transactionResult = result.getReply();

//        assertThat(transactionResult.transactionId()).isEqualTo("TX001");
//        assertThat(transactionResult.gainLoss()).isNull(); // No gain/loss for buy

        // Verify events - should have 1 initialization + 1 book cost adjusted
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
        var testKit = createTestKit("ACC003-GOOGL");

        // Initialize position
        var openingBalance = new OpeningBalance(
            "ACC003",
            "GOOGL",
            BigDecimal.valueOf(10),
            BigDecimal.valueOf(2500) // $250 per unit
        );

        testKit.method(PositionEntity::initializeFromOpeningBalance).invoke(openingBalance);

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
        var transactionResult = result.getReply();

//        assertThat(transactionResult.transactionId()).isEqualTo("TX002");

        // Verify gain/loss calculation
        // Net proceeds: 5 * 280 - 8 = 1392
        // Cost basis: 250 * 5 = 1250
        // Gain: 1392 - 1250 = 142
//        assertThat(transactionResult.gainLoss()).isNotNull();
//        assertThat(transactionResult.gainLoss().gainLossAmount()).isEqualByComparingTo(BigDecimal.valueOf(142));
//        assertThat(transactionResult.gainLoss().isGain()).isTrue();

        // Verify events - should have 1 initialization + 2 events (gain/loss + book cost adjusted)
        var allEvents = testKit.getAllEvents();
        assertThat(allEvents).hasSize(3);

        var gainLossEvent = (PositionEvent.GainLossIncurred) allEvents.get(1);
        assertThat(gainLossEvent.gainLossAmount()).isEqualByComparingTo(BigDecimal.valueOf(142));

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
        var testKit = createTestKit("ACC004-TSLA");

        // Initialize with some units
        var openingBalance = new OpeningBalance(
            "ACC004",
            "TSLA",
            BigDecimal.valueOf(20),
            BigDecimal.valueOf(4000)
        );

        testKit.method(PositionEntity::initializeFromOpeningBalance).invoke(openingBalance);

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
//        assertThat(result.getReply().gainLoss()).isNull(); // No gain/loss on transfer

        // Verify final state: 30 units, $6,500 cost
        var state = testKit.getState();
        assertThat(state.unitsHeld()).isEqualByComparingTo(BigDecimal.valueOf(30));
        assertThat(state.bookCost()).isEqualByComparingTo(BigDecimal.valueOf(6500)); // 4000 + (10*250)
        assertThat(state.centsPerUnit()).isEqualByComparingTo(BigDecimal.valueOf(216.6667)); // 6500 / 30
    }

    @Test
    public void shouldProcessCorporateActionTransaction() {
        var testKit = createTestKit("ACC005-META");

        // Initialize position
        var openingBalance = new OpeningBalance(
            "ACC005",
            "META",
            BigDecimal.valueOf(100),
            BigDecimal.valueOf(20000)
        );

        testKit.method(PositionEntity::initializeFromOpeningBalance).invoke(openingBalance);

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
//        assertThat(result.getReply().gainLoss()).isNull(); // No gain/loss on corporate action

        // Verify final state: same units, reduced book cost
        var state = testKit.getState();
        assertThat(state.unitsHeld()).isEqualByComparingTo(BigDecimal.valueOf(100)); // Units unchanged
        assertThat(state.bookCost()).isEqualByComparingTo(BigDecimal.valueOf(19500)); // 20000 - (100*5)
        assertThat(state.centsPerUnit()).isEqualByComparingTo(BigDecimal.valueOf(195)); // 19500 / 100
    }

    @Test
    public void shouldRejectDoubleInitialization() {
        var testKit = createTestKit("ACC006-NVDA");

        var openingBalance = new OpeningBalance(
            "ACC006",
            "NVDA",
            BigDecimal.valueOf(50),
            BigDecimal.valueOf(7500)
        );

        // First initialization should succeed
        var result1 = testKit.method(PositionEntity::initializeFromOpeningBalance).invoke(openingBalance);
        assertThat(result1.isReply()).isTrue();
        assertThat(result1.didPersistEvents()).isTrue();

        // Second initialization should fail
        var result2 = testKit.method(PositionEntity::initializeFromOpeningBalance).invoke(openingBalance);
        assertThat(result2.isReply()).isTrue();
        assertThat(result2.didPersistEvents()).isFalse();
    }

    @Test
    public void shouldHandleZeroOpeningBalance() {
        var testKit = createTestKit("ACC007-CRM");

        var zeroBalance = new OpeningBalance(
            "ACC007",
            "CRM",
            BigDecimal.ZERO,
            BigDecimal.ZERO
        );

        var result = testKit.method(PositionEntity::initializeFromOpeningBalance).invoke(zeroBalance);
        assertThat(result.isReply()).isTrue();

        var state = testKit.getState();
        assertThat(state.unitsHeld()).isEqualByComparingTo(BigDecimal.ZERO);
        assertThat(state.bookCost()).isEqualByComparingTo(BigDecimal.ZERO);
        assertThat(state.centsPerUnit()).isEqualByComparingTo(BigDecimal.ZERO);

        // Should be able to buy into zero position
        var buyTransaction = new Transaction(
            "TX005",
            "ACC007",
            "CRM",
            TransactionType.BUY,
            Instant.now(),
            BigDecimal.valueOf(25),
            BigDecimal.valueOf(300),
            BigDecimal.valueOf(12)
        );

        var buyResult = testKit.method(PositionEntity::processTransaction).invoke(buyTransaction);
        assertThat(buyResult.isReply()).isTrue();

        var finalState = testKit.getState();
        assertThat(finalState.unitsHeld()).isEqualByComparingTo(BigDecimal.valueOf(25));
        assertThat(finalState.bookCost()).isEqualByComparingTo(BigDecimal.valueOf(7512)); // (25*300) + 12
    }

    @Test
    public void shouldValidateEntityIdMatchesTransaction() {
        var testKit = createTestKit("ACC008-AMZN");

        // Initialize position
        var openingBalance = new OpeningBalance(
            "ACC008",
            "AMZN",
            BigDecimal.valueOf(10),
            BigDecimal.valueOf(3000)
        );

        testKit.method(PositionEntity::initializeFromOpeningBalance).invoke(openingBalance);

        // Try to process transaction for different account/instrument
        var wrongTransaction = new Transaction(
            "TX006",
            "ACC999", // Wrong account
            "AMZN",
            TransactionType.BUY,
            Instant.now(),
            BigDecimal.valueOf(5),
            BigDecimal.valueOf(350),
            BigDecimal.valueOf(8)
        );

        var result = testKit.method(PositionEntity::processTransaction).invoke(wrongTransaction);
        assertThat(result.isError()).isTrue();
        assertThat(result.getError()).contains("Transaction does not match position");
    }
}