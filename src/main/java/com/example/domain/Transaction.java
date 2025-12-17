package com.example.domain;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Represents a financial transaction loaded from the SQL database.
 * Contains all information needed to process book cost and gain/loss calculations.
 */
public record Transaction(
    String id,                  // Unique transaction identifier
    String accountId,           // Client account identifier
    String instrumentId,        // Financial instrument (AAPL, MSFT, etc.)
    TransactionType type,       // BUY, SELL, TRANSFER_IN, TRANSFER_OUT, CORPORATE_ACTION
    Instant dateTime,           // Transaction timestamp
    BigDecimal units,           // Units transacted
    BigDecimal price,           // Price per unit
    BigDecimal fees             // Transaction fees
) {

    public Transaction {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("Transaction ID cannot be null or blank");
        }
        if (accountId == null || accountId.isBlank()) {
            throw new IllegalArgumentException("Account ID cannot be null or blank");
        }
        if (instrumentId == null || instrumentId.isBlank()) {
            throw new IllegalArgumentException("Instrument ID cannot be null or blank");
        }
        if (type == null) {
            throw new IllegalArgumentException("Transaction type cannot be null");
        }
        if (dateTime == null) {
            throw new IllegalArgumentException("DateTime cannot be null");
        }
        if (units == null || units.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("Units must be positive");
        }
        if (price == null || price.compareTo(BigDecimal.ZERO) < 0) {
            throw new IllegalArgumentException("Price cannot be negative");
        }
        if (fees == null || fees.compareTo(BigDecimal.ZERO) < 0) {
            throw new IllegalArgumentException("Fees cannot be negative");
        }
    }

    /**
     * @return Type-safe position identifier
     */
    public PositionId positionId() {
        return new PositionId(accountId, instrumentId);
    }

    /**
     * @return Total transaction cost including fees (for buy transactions)
     */
    public BigDecimal totalCost() {
        return units.multiply(price).add(fees);
    }

    /**
     * @return Net proceeds after fees (for sell transactions)
     */
    public BigDecimal netProceeds() {
        return units.multiply(price).subtract(fees);
    }
}