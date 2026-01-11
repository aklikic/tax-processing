package com.example.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Optional;

/**
 * Domain model for OpeningBalanceBatch workflow state.
 * Tracks the progress of coordinating multiple OpeningBalanceTransactionsBatch sub-workflows.
 */
public record PositionBatchWindowState(
        String positionWindowId,
        int positionWindowOffset,
        int positionWindowLimit,
        long transactionCount,
        int transactionBatchCount,
        int transactionsPerBatch,
        String batchId,
        String taxYear,
        String parentWorkflowId,
        ProcessingStatus status,
        int transactionBatchOffset,
        int initRetries,
        int startRetries,
        Optional<String> errorMessage
) {

    public enum ProcessingStatus {
        PENDING, INIT, START, RUNNING, COMPLETED, FAILED
    }


    public static PositionBatchWindowState empty() {
        return new PositionBatchWindowState(
            "",
          0,
            0,
            0,
            0,
            0,
            "",
            "",
            "",
            ProcessingStatus.PENDING,
            -1,
            0,
            0,
            Optional.empty()
        );
    }

    @JsonIgnore
    public boolean isEmpty() {
        return positionWindowId.isBlank();
    }

    public static PositionBatchWindowState init(String positionWindowId, int positionWindowOffset, int positionWindowLimit, String batchId, String taxYear, String parentWorkflowId) {
        return new PositionBatchWindowState(
                positionWindowId,
                positionWindowOffset,
                positionWindowLimit,
                0,
                0,
                0,
                batchId,
                taxYear,
                parentWorkflowId,
                ProcessingStatus.INIT,
                -1,
                0,
                0,
                Optional.empty()
        );
    }

    public PositionBatchWindowState start( long transactionCount, int transactionBatchCount, int transactionsPerBatch) {
        return new PositionBatchWindowState(positionWindowId, positionWindowOffset, positionWindowLimit, transactionCount, transactionBatchCount, transactionsPerBatch, batchId, taxYear, parentWorkflowId, ProcessingStatus.START, transactionBatchOffset, initRetries, startRetries, errorMessage);
    }
    public PositionBatchWindowState withTransactionBatchOffset(int offset) {
        return new PositionBatchWindowState(positionWindowId, positionWindowOffset, positionWindowLimit, transactionCount, transactionBatchCount, transactionsPerBatch, batchId, taxYear, parentWorkflowId, status, offset, initRetries, startRetries, errorMessage);
    }

    public PositionBatchWindowState withStatus(ProcessingStatus processingStatus) {
        return new PositionBatchWindowState(positionWindowId, positionWindowOffset, positionWindowLimit,transactionCount, transactionBatchCount, transactionsPerBatch, batchId, taxYear, parentWorkflowId, processingStatus, transactionBatchOffset, initRetries, startRetries, Optional.empty());
    }

    public PositionBatchWindowState withError(String newErrorMessage) {
        return new PositionBatchWindowState(positionWindowId, positionWindowOffset, positionWindowLimit, transactionCount, transactionBatchCount, transactionsPerBatch, batchId, taxYear, parentWorkflowId, ProcessingStatus.FAILED, transactionBatchOffset, initRetries, startRetries, Optional.of(newErrorMessage));
    }
    public PositionBatchWindowState withInitRetriesIncrease() {
        return new PositionBatchWindowState(positionWindowId, positionWindowOffset, positionWindowLimit, transactionCount, transactionBatchCount, transactionsPerBatch, batchId, taxYear, parentWorkflowId, status, transactionBatchOffset, initRetries + 1, startRetries , errorMessage);
    }
    public PositionBatchWindowState withStartRetriesIncrease() {
        return new PositionBatchWindowState(positionWindowId, positionWindowOffset, positionWindowLimit, transactionCount, transactionBatchCount, transactionsPerBatch, batchId, taxYear, parentWorkflowId, status, transactionBatchOffset, initRetries, startRetries +1 , errorMessage);
    }

}