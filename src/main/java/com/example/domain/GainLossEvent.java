package com.example.domain;

import java.math.BigDecimal;

/**
 * Represents a gain or loss event from a sell transaction.
 * Used for reporting and downstream processing.
 */
public record GainLossEvent(
    PositionId positionId,
    Transaction transaction,
    BigDecimal gainLossAmount  // Positive = gain, negative = loss
) {

    public GainLossEvent {
        if (positionId == null) {
            throw new IllegalArgumentException("Position ID cannot be null");
        }
        if (transaction == null) {
            throw new IllegalArgumentException("Transaction cannot be null");
        }
        if (gainLossAmount == null) {
            throw new IllegalArgumentException("Gain/loss amount cannot be null");
        }
        if (transaction.type() != TransactionType.SELL) {
            throw new IllegalArgumentException("Gain/loss events can only be created for sell transactions");
        }
    }

    /**
     * @return true if this represents a gain (positive amount)
     */
    public boolean isGain() {
        return gainLossAmount.compareTo(BigDecimal.ZERO) > 0;
    }

    /**
     * @return true if this represents a loss (negative amount)
     */
    public boolean isLoss() {
        return gainLossAmount.compareTo(BigDecimal.ZERO) < 0;
    }
}