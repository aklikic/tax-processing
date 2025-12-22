package com.example.domain;

import java.util.List;

/**
 * State for OpeningBalanceTransactionsBatch workflow.
 * Tracks processing of transactions for a batch of opening balances.
 */
public record OpeningBalanceTransactionsBatchState(
    String batchId,
    String taxYear,
    List<PositionId> positionIds,
    ProcessingStatus status,
    int totalPositions,
    int processedTransactions,
    String parentWorkflowId,
    String errorMessage
) {

    public enum ProcessingStatus {
        INITIALIZING,
        PROCESSING_TRANSACTIONS,
        COMPLETED,
        FAILED
    }

    public OpeningBalanceTransactionsBatchState {
        if (batchId == null || batchId.isBlank()) {
            throw new IllegalArgumentException("Batch ID cannot be null or blank");
        }
        if (taxYear == null || taxYear.isBlank()) {
            throw new IllegalArgumentException("Tax year cannot be null or blank");
        }
        if (positionIds == null) {
            throw new IllegalArgumentException("Position IDs cannot be null");
        }
        if (status == null) {
            throw new IllegalArgumentException("Status cannot be null");
        }
        if (totalPositions < 0) {
            throw new IllegalArgumentException("Total positions cannot be negative");
        }
    }

    /**
     * Create initial state for a new batch.
     */
    public static OpeningBalanceTransactionsBatchState init(String batchId, String taxYear, List<PositionId> positionIds, String parentWorkflowId) {
        return new OpeningBalanceTransactionsBatchState(
            batchId,
            taxYear,
            positionIds,
            ProcessingStatus.INITIALIZING,
            positionIds.size(),
            0,
            parentWorkflowId,
            null
        );
    }

    /**
     * Create initial state for a new batch with parent workflow callback.
     */
//    public static OpeningBalanceTransactionsBatchState initWithParent(String batchId, String taxYear, List<PositionId> positionIds, String parentWorkflowId) {
//        return new OpeningBalanceTransactionsBatchState(
//            batchId,
//            taxYear,
//            positionIds,
//
//            ProcessingStatus.INITIALIZING,
//            positionIds.size(),
//            0,
//            0,
//            parentWorkflowId,
//            null
//        );
//    }

    /**
     * Update status.
     */
    public OpeningBalanceTransactionsBatchState withStatus(ProcessingStatus newStatus) {
        return new OpeningBalanceTransactionsBatchState(
            batchId,
            taxYear,
            positionIds,
            newStatus,
            totalPositions,
            processedTransactions,
            parentWorkflowId,
            errorMessage
        );
    }

    /**
     * Update processed transactions count.
     */
    public OpeningBalanceTransactionsBatchState withProcessedTransactions(int newProcessedTransactions) {
        return new OpeningBalanceTransactionsBatchState(
            batchId,
            taxYear,
            positionIds,
            status,
            totalPositions,
            newProcessedTransactions,
            parentWorkflowId,
            errorMessage
        );
    }

    /**
     * Mark batch as failed with error message.
     */
    public OpeningBalanceTransactionsBatchState withError(String error) {
        return new OpeningBalanceTransactionsBatchState(
            batchId,
            taxYear,
            positionIds,
            ProcessingStatus.FAILED,
            totalPositions,
            processedTransactions,
            parentWorkflowId,
            error
        );
    }

}