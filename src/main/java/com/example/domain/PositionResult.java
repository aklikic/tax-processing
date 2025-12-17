package com.example.domain;

import java.util.List;

/**
 * Result of processing a transaction against a position.
 * Contains the updated position, events to persist, and optional gain/loss information.
 */
public record PositionResult(
    Position updatedPosition,
    List<PositionEvent> events,
    GainLossEvent gainLoss  // null for non-sell transactions
) {

    public PositionResult {
        if (updatedPosition == null) {
            throw new IllegalArgumentException("Updated position cannot be null");
        }
        if (events == null || events.isEmpty()) {
            throw new IllegalArgumentException("Events cannot be null or empty");
        }
    }
}