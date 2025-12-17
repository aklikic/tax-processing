package com.example.domain;

import akka.javasdk.annotations.TypeName;
import java.math.BigDecimal;

/**
 * Event sourced events from PositionEntity.
 * These events are persisted and consumed by views and consumers.
 */
public sealed interface PositionEvent permits
    PositionEvent.Initialized,
    PositionEvent.BookCostAdjusted,
    PositionEvent.GainLossIncurred {

    @TypeName("position-initialized")
    record Initialized(
        PositionId positionId,
        BigDecimal initialUnits,
        BigDecimal initialBookCost,
        BigDecimal initialCentsPerUnit
    ) implements PositionEvent {

        public Initialized {
            if (positionId == null) {
                throw new IllegalArgumentException("Position ID cannot be null");
            }
            if (initialUnits == null || initialUnits.compareTo(BigDecimal.ZERO) < 0) {
                throw new IllegalArgumentException("Initial units cannot be negative");
            }
            if (initialBookCost == null || initialBookCost.compareTo(BigDecimal.ZERO) < 0) {
                throw new IllegalArgumentException("Initial book cost cannot be negative");
            }
            if (initialCentsPerUnit == null || initialCentsPerUnit.compareTo(BigDecimal.ZERO) < 0) {
                throw new IllegalArgumentException("Initial cents per unit cannot be negative");
            }
        }
    }

    @TypeName("book-cost-adjusted")
    record BookCostAdjusted(
        PositionId positionId,
        Transaction transaction,
        BigDecimal unitsHeld,
        BigDecimal bookCost,
        BigDecimal centsPerUnit
    ) implements PositionEvent {

        public BookCostAdjusted {
            if (positionId == null) {
                throw new IllegalArgumentException("Position ID cannot be null");
            }
            if (transaction == null) {
                throw new IllegalArgumentException("Transaction cannot be null");
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
        }
    }

    @TypeName("gain-loss-incurred")
    record GainLossIncurred(
        PositionId positionId,
        Transaction transaction,
        BigDecimal gainLossAmount
    ) implements PositionEvent {

        public GainLossIncurred {
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
}