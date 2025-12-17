package com.example.domain;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Represents opening balance data for a position at the start of a tax year.
 * Loaded from the SQL database and used to initialize Position entities.
 */
public record OpeningBalance(
    String accountId,           // Client account identifier
    String instrumentId,        // Financial instrument (AAPL, MSFT, etc.)
    BigDecimal openingUnits,    // Units held at start of tax year
    BigDecimal openingCost      // Cost basis at start of tax year
) {

    public OpeningBalance {
        if (accountId == null || accountId.isBlank()) {
            throw new IllegalArgumentException("Account ID cannot be null or blank");
        }
        if (instrumentId == null || instrumentId.isBlank()) {
            throw new IllegalArgumentException("Instrument ID cannot be null or blank");
        }
        if (openingUnits == null || openingUnits.compareTo(BigDecimal.ZERO) < 0) {
            throw new IllegalArgumentException("Opening units cannot be negative");
        }
        if (openingCost == null || openingCost.compareTo(BigDecimal.ZERO) < 0) {
            throw new IllegalArgumentException("Opening cost cannot be negative");
        }
        // Allow zero units and cost for positions that were closed
    }

    /**
     * @return Type-safe position identifier
     */
    public PositionId positionId() {
        return new PositionId(accountId, instrumentId);
    }

    /**
     * Converts this opening balance to an initial Position.
     * @param cacheSize the size for the transaction idempotency cache
     * @return Position ready for transaction processing
     */
    public Position toInitialPosition(int cacheSize) {
        var emptyCache = BoundedTransactionIdCache.empty(cacheSize);

        if (openingUnits.compareTo(BigDecimal.ZERO) == 0) {
            // No opening units - start with zero position
            return new Position(accountId, instrumentId, BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, emptyCache);
        }

        var centsPerUnit = openingCost.divide(openingUnits, 4, RoundingMode.HALF_UP);
        return new Position(accountId, instrumentId, openingUnits, openingCost, centsPerUnit, emptyCache);
    }
}