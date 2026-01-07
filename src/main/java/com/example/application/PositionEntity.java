package com.example.application;

import akka.Done;
import akka.javasdk.annotations.Component;
import akka.javasdk.eventsourcedentity.EventSourcedEntity;
import akka.javasdk.eventsourcedentity.EventSourcedEntityContext;
import com.example.domain.*;

/**
 * Event Sourced Entity managing position state for a specific account-instrument combination.
 * Entity ID format: "{accountId}-{instrumentId}"
 *
 * Handles:
 * - Position initialization from opening balances
 * - Transaction processing with chronological ordering
 * - Book cost calculations and gain/loss tracking
 * - Event emission for downstream consumers
 */
@Component(id = "position")
public class PositionEntity extends EventSourcedEntity<Position, PositionEvent> {

    private final EventSourcedEntityContext context;
    private final ProcessingConfig processingConfig;

    public PositionEntity(EventSourcedEntityContext context, ProcessingConfig processingConfig) {
        this.context = context;
        this.processingConfig = processingConfig;
    }

    @Override
    public Position emptyState() {
        // Parse entity ID to get account and instrument
        var positionId = PositionId.fromEntityId(context.entityId());
        return new Position(
            positionId.accountId(),
            positionId.instrumentId(),
            java.math.BigDecimal.ZERO,
            java.math.BigDecimal.ZERO,
            java.math.BigDecimal.ZERO,
            BoundedTransactionIdCache.empty(processingConfig.positionIdempotencyCacheSize())
        );
    }

    /**
     * Initialize position from opening balance.
     * Called once when the position entity is first created.
     */
    public Effect<Done> initializeFromOpeningBalance(OpeningBalance openingBalance) {
        if (!currentState().unitsHeld().equals(java.math.BigDecimal.ZERO) ||
            !currentState().bookCost().equals(java.math.BigDecimal.ZERO)) {
            return effects().reply(Done.getInstance());
        }

        var positionId = openingBalance.positionId();
        if (!positionId.toEntityId().equals(context.entityId())) {
            return effects().error("Opening balance does not match entity ID");
        }

        var initialPosition = openingBalance.toInitialPosition(processingConfig.positionIdempotencyCacheSize());
        var event = new PositionEvent.Initialized(
            positionId,
            initialPosition.unitsHeld(),
            initialPosition.bookCost(),
            initialPosition.centsPerUnit()
        );

        return effects()
            .persist(event)
            .thenReply(state -> Done.getInstance());
    }

    /**
     * Process a single transaction against this position.
     * Transactions must be processed in chronological order for accurate calculations.
     */
    public Effect<Done> processTransaction(Transaction transaction) {
        try {
            var positionResult = currentState().processTransaction(transaction);

            return effects()
                .persistAll(positionResult.events())
                .thenReply(state -> Done.done());
        } catch (IllegalArgumentException | IllegalStateException e) {
            return effects().error("Transaction processing failed: " + e.getMessage());
        }
    }

    /**
     * Get current position state.
     * Used for debugging and monitoring.
     */
    public Effect<Position> getCurrentState() {
        return effects().reply(currentState());
    }

    /**
     * Event handlers - pure functions that update state based on events
     */
    @Override
    public Position applyEvent(PositionEvent event) {
        return switch (event) {
            case PositionEvent.Initialized initialized -> {
                yield new Position(
                    initialized.positionId().accountId(),
                    initialized.positionId().instrumentId(),
                    initialized.initialUnits(),
                    initialized.initialBookCost(),
                    initialized.initialCentsPerUnit(),
                    BoundedTransactionIdCache.empty(processingConfig.positionIdempotencyCacheSize())
                );
            }

            case PositionEvent.BookCostAdjusted adjusted ->
                new Position(
                    currentState().accountId(),
                    currentState().instrumentId(),
                    adjusted.unitsHeld(),
                    adjusted.bookCost(),
                    adjusted.centsPerUnit(),
                    currentState().processedTransactionIds()
                );

            case PositionEvent.GainLossIncurred ignored ->
                // Gain/loss events don't change position state directly
                // The accompanying BookCostAdjusted event updates the state
                currentState();
        };
    }

    /**
     * Result of processing a transaction.
     * Contains transaction ID, optional gain/loss, and updated position state.
     */
    public record TransactionResult(
        String transactionId,
        GainLossEvent gainLoss, // null for non-sell transactions
        Position updatedPosition
    ) {}
}