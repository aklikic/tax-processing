package com.example.domain;

import java.util.Optional;

/**
 * Domain model for OpeningBalanceBatch workflow state.
 * Tracks the progress of coordinating multiple OpeningBalanceTransactionsBatch sub-workflows.
 */
public record TransactionBatchWindowState(
        String windowId,
        int windowOffset,
        int windowLimit,
        int microBatchCount,
        int transPerMicroBatch,
        String batchId,
        String taxYear,
        String parentWorkflowId,
        ProcessingStatus status,
        int transactionOffset,
        Optional<String> errorMessage
) {

    public enum ProcessingStatus {
        PENDING, START, RUNNING, COMPLETED, FAILED
    }


    public static TransactionBatchWindowState empty() {
        return new TransactionBatchWindowState(
            "",
          0,
            0,
            0,
            0,
            "",
            "",
            "",
            ProcessingStatus.PENDING,
            0,
            java.util.Optional.empty()
        );
    }

    public boolean isEmpty() {
        return windowId.isBlank();
    }

    public static TransactionBatchWindowState start(String windowId, int windowOffset, int windowLimit, int microBatchCount, int transPerMicroBatch, String batchId, String taxYear, String parentWorkflowId) {
        return new TransactionBatchWindowState(
                windowId,
                windowOffset,
                windowLimit,
                microBatchCount,
                transPerMicroBatch,
                batchId,
                taxYear,
                parentWorkflowId,
                ProcessingStatus.START,
                windowOffset,
                java.util.Optional.empty()
        );
    }
    public TransactionBatchWindowState withOffset(int offset) {
        return new TransactionBatchWindowState(windowId, windowOffset, windowLimit, microBatchCount, transPerMicroBatch, batchId, taxYear, parentWorkflowId, status, offset, errorMessage);
    }

    public TransactionBatchWindowState withStatus(ProcessingStatus processingStatus) {
        return new TransactionBatchWindowState(windowId, windowOffset, windowLimit,microBatchCount, transPerMicroBatch, batchId, taxYear, parentWorkflowId, processingStatus, transactionOffset, errorMessage);
    }

    public TransactionBatchWindowState withError(String newErrorMessage) {
        return new TransactionBatchWindowState(windowId, windowOffset, windowLimit, microBatchCount, transPerMicroBatch, batchId, taxYear, parentWorkflowId, ProcessingStatus.FAILED, transactionOffset, Optional.of(newErrorMessage));
    }

}