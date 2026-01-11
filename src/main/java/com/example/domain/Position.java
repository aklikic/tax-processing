package com.example.domain;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

/**
 * Represents the current position state for a specific account-instrument combination.
 * Contains business logic for processing different transaction types.
 */
public record Position(
    String accountId,
    String instrumentId,
    BigDecimal unitsHeld,       // Current units held
    BigDecimal bookCost,        // Current book cost
    BigDecimal centsPerUnit,    // Current cost per unit
    BoundedTransactionIdCache processedTransactionIds  // FIFO cache of processed transaction IDs
) {
    /**
     * @return Type-safe position identifier
     */
    public PositionId positionId() {
        return new PositionId(accountId, instrumentId);
    }

    /**
     * Processes a transaction and returns the updated position with events.
     * Implements idempotency - if transaction was already processed, returns no-op result.
     * @param transaction the transaction to process
     * @return PositionResult containing updated position, events, and optional gain/loss
     */
    public PositionResult processTransaction(Transaction transaction) {
        if (!accountId.equals(transaction.accountId()) || !instrumentId.equals(transaction.instrumentId())) {
            return new PositionResult(List.of());
        }

        // Idempotency check - if transaction already processed, return no-op result
        if (processedTransactionIds.contains(transaction.id())) {
            return new PositionResult(List.of());
        }

        return switch (transaction.type()) {
            case BUY -> processBuy(transaction);
            case SELL -> processSell(transaction);
            case TRANSFER_IN -> processTransferIn(transaction);
            case TRANSFER_OUT -> processTransferOut(transaction);
            case CORPORATE_ACTION -> processCorporateAction(transaction);
        };
    }

    /**
     * Process a BUY transaction.
     * Increases units and book cost, recalculates cents per unit.
     */
    public PositionResult processBuy(Transaction tx) {
        var newUnits = unitsHeld.add(tx.units());
        var newBookCost = bookCost.add(tx.totalCost());
        var newCentsPerUnit = newBookCost.divide(newUnits, 4, RoundingMode.HALF_UP);
        var event = new PositionEvent.BookCostAdjusted(positionId(), tx, newUnits, newBookCost, newCentsPerUnit);

        return new PositionResult(List.of(event));
    }

    /**
     * Process a SELL transaction.
     * Calculates gain/loss and reduces position.
     */
    public PositionResult processSell(Transaction tx) {
        if (tx.units().compareTo(unitsHeld) > 0) {
            return new PositionResult( List.of());
        }

        var netProceeds = tx.netProceeds();
        var costBasis = centsPerUnit.multiply(tx.units());
        var gainLoss = netProceeds.subtract(costBasis);

        var newUnits = unitsHeld.subtract(tx.units());
        var newBookCost = bookCost.subtract(costBasis);

        List<PositionEvent> events = List.of(
            new PositionEvent.GainLossIncurred(positionId(), tx, gainLoss),
            new PositionEvent.BookCostAdjusted(positionId(), tx, newUnits, newBookCost, centsPerUnit)
        );

        return new PositionResult(events);
    }

    /**
     * Process a TRANSFER_IN transaction.
     * Similar to buy but uses transfer price.
     */
    public PositionResult processTransferIn(Transaction tx) {
        var newUnits = unitsHeld.add(tx.units());
        var transferCost = tx.units().multiply(tx.price());
        var newBookCost = bookCost.add(transferCost);
        var newCentsPerUnit = newBookCost.divide(newUnits, 4, RoundingMode.HALF_UP);
        var event = new PositionEvent.BookCostAdjusted(positionId(), tx, newUnits, newBookCost, newCentsPerUnit);

        return new PositionResult(List.of(event));
    }

    /**
     * Process a TRANSFER_OUT transaction.
     * Reduces position using transfer price.
     */
    public PositionResult processTransferOut(Transaction tx) {
        if (tx.units().compareTo(unitsHeld) > 0) {
            return new PositionResult(List.of());
        }

        var newUnits = unitsHeld.subtract(tx.units());
        var transferCost = tx.units().multiply(tx.price());
        var newBookCost = bookCost.subtract(transferCost);
        var newCentsPerUnit = newUnits.compareTo(BigDecimal.ZERO) == 0 ?
            BigDecimal.ZERO : newBookCost.divide(newUnits, 4, RoundingMode.HALF_UP);

        var event = new PositionEvent.BookCostAdjusted(positionId(), tx, newUnits, newBookCost, newCentsPerUnit);

        return new PositionResult(List.of(event));
    }

    /**
     * Process a CORPORATE_ACTION transaction (cash only).
     * Reduces book cost without changing units.
     */
    public PositionResult processCorporateAction(Transaction tx) {
        var cashReceived = tx.units().multiply(tx.price());
        var newBookCost = bookCost.subtract(cashReceived);
        var newCentsPerUnit = unitsHeld.compareTo(BigDecimal.ZERO) == 0 ?
            BigDecimal.ZERO : newBookCost.divide(unitsHeld, 4, RoundingMode.HALF_UP);

        var event = new PositionEvent.BookCostAdjusted(positionId(), tx, unitsHeld, newBookCost, newCentsPerUnit);

        return new PositionResult( List.of(event));
    }

    public Position onBookCostAdjustedEvent(PositionEvent.BookCostAdjusted event){
        return new Position(
                accountId,
                instrumentId,
                event.unitsHeld(),
                event.bookCost(),
                event.centsPerUnit(),
                processedTransactionIds().add(event.transaction().id())
        );
    }
    public Position onGainLossIncurredEvent(PositionEvent.GainLossIncurred event){
        return new Position(
                accountId,
                instrumentId,
                unitsHeld,
                bookCost,
                centsPerUnit,
                processedTransactionIds().add(event.transaction().id())
        );
    }

}