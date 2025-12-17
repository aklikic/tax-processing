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

    public Position {
        if (accountId == null || accountId.isBlank()) {
            throw new IllegalArgumentException("Account ID cannot be null or blank");
        }
        if (instrumentId == null || instrumentId.isBlank()) {
            throw new IllegalArgumentException("Instrument ID cannot be null or blank");
        }
        if (unitsHeld == null || unitsHeld.compareTo(BigDecimal.ZERO) < 0) {
            throw new IllegalArgumentException("Units held cannot be negative");
        }
        if (bookCost == null || bookCost.compareTo(BigDecimal.ZERO) < 0) {
            throw new IllegalArgumentException("Book cost cannot be negative");
        }
        if (centsPerUnit == null || centsPerUnit.compareTo(BigDecimal.ZERO) < 0) {
            throw new IllegalArgumentException("Cents per unit cannot be negative");
        }
        if (processedTransactionIds == null) {
            throw new IllegalArgumentException("Processed transaction IDs cannot be null");
        }
    }

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
            throw new IllegalArgumentException("Transaction does not match position");
        }

        // Idempotency check - if transaction already processed, return no-op result
        if (processedTransactionIds.contains(transaction.id())) {
            return new PositionResult(this, List.of(), null);
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

        var updatedTransactionIds = processedTransactionIds.add(tx.id());
        var updatedPosition = new Position(accountId, instrumentId, newUnits, newBookCost, newCentsPerUnit, updatedTransactionIds);
        var event = new PositionEvent.BookCostAdjusted(positionId(), tx, newUnits, newBookCost, newCentsPerUnit);

        return new PositionResult(updatedPosition, List.of(event), null);
    }

    /**
     * Process a SELL transaction.
     * Calculates gain/loss and reduces position.
     */
    public PositionResult processSell(Transaction tx) {
        if (tx.units().compareTo(unitsHeld) > 0) {
            throw new IllegalStateException("Cannot sell more units than held");
        }

        var netProceeds = tx.netProceeds();
        var costBasis = centsPerUnit.multiply(tx.units());
        var gainLoss = netProceeds.subtract(costBasis);

        var newUnits = unitsHeld.subtract(tx.units());
        var newBookCost = bookCost.subtract(costBasis);

        var updatedTransactionIds = processedTransactionIds.add(tx.id());
        var updatedPosition = new Position(accountId, instrumentId, newUnits, newBookCost, centsPerUnit, updatedTransactionIds);
        List<PositionEvent> events = List.of(
            new PositionEvent.GainLossIncurred(positionId(), tx, gainLoss),
            new PositionEvent.BookCostAdjusted(positionId(), tx, newUnits, newBookCost, centsPerUnit)
        );

        return new PositionResult(updatedPosition, events, new GainLossEvent(positionId(), tx, gainLoss));
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

        var updatedTransactionIds = processedTransactionIds.add(tx.id());
        var updatedPosition = new Position(accountId, instrumentId, newUnits, newBookCost, newCentsPerUnit, updatedTransactionIds);
        var event = new PositionEvent.BookCostAdjusted(positionId(), tx, newUnits, newBookCost, newCentsPerUnit);

        return new PositionResult(updatedPosition, List.of(event), null);
    }

    /**
     * Process a TRANSFER_OUT transaction.
     * Reduces position using transfer price.
     */
    public PositionResult processTransferOut(Transaction tx) {
        if (tx.units().compareTo(unitsHeld) > 0) {
            throw new IllegalStateException("Cannot transfer out more units than held");
        }

        var newUnits = unitsHeld.subtract(tx.units());
        var transferCost = tx.units().multiply(tx.price());
        var newBookCost = bookCost.subtract(transferCost);
        var newCentsPerUnit = newUnits.compareTo(BigDecimal.ZERO) == 0 ?
            BigDecimal.ZERO : newBookCost.divide(newUnits, 4, RoundingMode.HALF_UP);

        var updatedTransactionIds = processedTransactionIds.add(tx.id());
        var updatedPosition = new Position(accountId, instrumentId, newUnits, newBookCost, newCentsPerUnit, updatedTransactionIds);
        var event = new PositionEvent.BookCostAdjusted(positionId(), tx, newUnits, newBookCost, newCentsPerUnit);

        return new PositionResult(updatedPosition, List.of(event), null);
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

        var updatedTransactionIds = processedTransactionIds.add(tx.id());
        var updatedPosition = new Position(accountId, instrumentId, unitsHeld, newBookCost, newCentsPerUnit, updatedTransactionIds);
        var event = new PositionEvent.BookCostAdjusted(positionId(), tx, unitsHeld, newBookCost, newCentsPerUnit);

        return new PositionResult(updatedPosition, List.of(event), null);
    }
}