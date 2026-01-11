package com.example.application;

import akka.javasdk.annotations.Component;
import akka.javasdk.annotations.Consume;
import akka.javasdk.annotations.Query;
import akka.javasdk.view.TableUpdater;
import akka.javasdk.view.View;
import com.example.domain.PositionEvent;
import com.example.domain.PositionId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * View that tracks PositionEntity processing status for completion monitoring.
 * Subscribes to PositionEntity events to provide real-time processing statistics.
 */
@Component(id = "position-processing-status")
public class PositionProcessingStatusView extends View {

    private static final Logger logger = LoggerFactory.getLogger(PositionProcessingStatusView.class);

    public record PositionStatusEntry(
        String positionId,              // Format: "accountId-instrumentId"
        String accountId,
        String instrumentId,
        boolean initialized,            // Whether position was initialized from opening balance
        int transactionsProcessed,      // Total number of transactions processed
        BigDecimal currentUnitsHeld,    // Current units held
        BigDecimal currentBookCost,     // Current book cost
        BigDecimal totalGainLoss,       // Total realized gain/loss
        Optional<Instant> lastTransactionTime,    // Timestamp of last processed transaction
        Instant lastUpdated            // When this record was last updated
    ) {

        public PositionStatusEntry withTransaction(PositionEvent.BookCostAdjusted event) {
            return new PositionStatusEntry(
                positionId,
                accountId,
                instrumentId,
                initialized,
                transactionsProcessed + 1,
                event.unitsHeld(),
                event.bookCost(),
                totalGainLoss,
                Optional.of(event.transaction().dateTime()),
                Instant.now()
            );
        }

        public PositionStatusEntry withGainLoss(PositionEvent.GainLossIncurred event) {
            return new PositionStatusEntry(
                positionId,
                accountId,
                instrumentId,
                initialized,
                    transactionsProcessed + 1,
                currentUnitsHeld,
                currentBookCost,
                totalGainLoss.add(event.gainLossAmount()),
                Optional.of(event.transaction().dateTime()),
                Instant.now()
            );
        }
    }
    public record PositionsProcessingTotalCount(int totalCount){}

    public record PositionStatusResult(List<PositionStatusEntry> positions) {}

    public record AccountSummary(
            String accountId,
            int totalPositions,
            int totalTransactions,
            BigDecimal totalGainLoss,
            Instant lastActivity
    ) {}

    public record AccountSummaryResult(List<AccountSummary> accounts) {}

    @Consume.FromEventSourcedEntity(PositionEntity.class)
    public static class PositionStatusTableUpdater extends TableUpdater<PositionStatusEntry> {

        private static final Logger logger = LoggerFactory.getLogger(PositionStatusTableUpdater.class);

        public Effect<PositionStatusEntry> onEvent(PositionEvent event) {
            var entityId = updateContext().eventSubject().orElse("");
            var positionId = PositionId.fromEntityId(entityId);

            return switch (event) {
//                case PositionEvent.Initialized initialized -> {
//                    logger.debug("Position initialized: {}", entityId);
//                    var entry = new PositionStatusEntry(
//                        entityId,
//                        positionId.accountId(),
//                        positionId.instrumentId(),
//                        true,
//                        0,
//                        initialized.initialUnits(),
//                        initialized.initialBookCost(),
//                        BigDecimal.ZERO,
//                        Optional.empty(),
//                        Instant.now()
//                    );
//                    yield effects().updateRow(entry);
//                }

                case PositionEvent.BookCostAdjusted adjusted -> {
                    logger.debug("Book cost adjusted for position: {}", entityId);
                    var currentEntry = rowState();
                    if (currentEntry == null) {
                        // Position not yet initialized - create entry
                        currentEntry = new PositionStatusEntry(
                            entityId,
                            positionId.accountId(),
                            positionId.instrumentId(),
                            false,
                            0,
                            BigDecimal.ZERO,
                            BigDecimal.ZERO,
                            BigDecimal.ZERO,
                            Optional.empty(),
                            Instant.now()
                        );
                    }
                    yield effects().updateRow(currentEntry.withTransaction(adjusted));
                }

                case PositionEvent.GainLossIncurred gainLoss -> {
                    logger.debug("Gain/loss incurred for position: {} amount: {}",
                                entityId, gainLoss.gainLossAmount());
                    var currentEntry = rowState();
                    if (currentEntry == null) {
                        // Position not yet tracked - create minimal entry
                        currentEntry = new PositionStatusEntry(
                            entityId,
                            positionId.accountId(),
                            positionId.instrumentId(),
                            false,
                            0,
                            BigDecimal.ZERO,
                            BigDecimal.ZERO,
                            BigDecimal.ZERO,
                            Optional.empty(),
                            Instant.now()
                        );
                    }
                    yield effects().updateRow(currentEntry.withGainLoss(gainLoss));
                }
            };
        }
    }

    /**
     * Get processing status for all positions.
     */
    @Query("SELECT total_count() AS totalCount FROM position_processing_status")
    public QueryEffect<PositionsProcessingTotalCount> getAllPositionsCount() {
        return queryResult();
    }

    /**
     * Get processing status for positions of a specific account.
     */
    @Query("SELECT total_count() AS totalCount FROM position_processing_status WHERE accountId = :accountId")
    public QueryEffect<PositionsProcessingTotalCount> getPositionsByAccountCount(String accountId) {
        return queryResult();
    }

    /**
     * Get processing status for a specific position.
     */
    @Query("SELECT * FROM position_processing_status WHERE positionId = :positionId")
    public QueryEffect<PositionStatusEntry> getPosition(String positionId) {
        return queryResult();
    }

    /**
     * Get positions that have been initialized but not yet processed any transactions.
     */
    @Query("SELECT total_count() AS totalCount FROM position_processing_status WHERE initialized = true AND transactionsProcessed = 0")
    public QueryEffect<PositionsProcessingTotalCount> getUnprocessedPositionsCount() {
        return queryResult();
    }

    /**
     * Get positions with specific transaction count.
     */
    @Query("SELECT total_count() AS totalCount FROM position_processing_status WHERE transactionsProcessed = :count")
    public QueryEffect<PositionsProcessingTotalCount> getPositionsCountByTransactionCount(int count) {
        return queryResult();
    }
}