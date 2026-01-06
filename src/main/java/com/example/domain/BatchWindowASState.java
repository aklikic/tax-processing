package com.example.domain;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Domain model for OpeningBalanceBatch workflow state.
 * Tracks the progress of coordinating multiple OpeningBalanceTransactionsBatch sub-workflows.
 */
public record BatchWindowASState(
        String windowId,
        int windowOffset,
        int windowLimit,
        String batchId,
        String taxYear,
        String parentWorkflowId,
        ProcessingStatus status,
        int processedPositionsOffset,
        int processedTransactionOffset,
        int completedPositions,
        Optional<String> errorMessage
) {

    public enum ProcessingStatus {
        PENDING, INITIALIZING_POSITIONS, LAUNCHING_TRANSACTION_PROCESSING, COMPLETED, FAILED
    }


    public static BatchWindowASState empty() {
        return new BatchWindowASState(
            "",
          0,
            0,
            "",
            "",
            "",
            ProcessingStatus.PENDING,
            0,
             0,
            0,
            Optional.empty()
        );
    }

    public boolean isEmpty() {
        return windowId.isBlank();
    }

    public static BatchWindowASState initialize(String windowId, int windowOffset, int windowLimit, String batchId, String taxYear, String parentWorkflowId) {
        return new BatchWindowASState(
                windowId,
                windowOffset,
                windowLimit,
                batchId,
                taxYear,
                parentWorkflowId,
                ProcessingStatus.INITIALIZING_POSITIONS,
                0,
                0,
                0,
                Optional.empty()
        );
    }

    public BatchWindowASState withOpeningBalances(List<OpeningBalance> balances){
        return new BatchWindowASState(windowId, windowOffset, windowLimit, batchId, taxYear, parentWorkflowId, status, balances, completedPositions, errorMessage);
    }

    public BatchWindowASState withError(int completedPositions, String error) {
        return new BatchWindowASState(windowId, windowOffset, windowLimit, batchId, taxYear, parentWorkflowId, ProcessingStatus.FAILED, openingBalances, completedPositions, Optional.of(error));
    }
    public BatchWindowASState withCompleted(int completedPositions) {
        return new BatchWindowASState(windowId, windowOffset, windowLimit, batchId, taxYear, parentWorkflowId, ProcessingStatus.FAILED, openingBalances, completedPositions, Optional.empty());
    }

    public BatchWindowASState withStatus(ProcessingStatus newStatus) {
        return new BatchWindowASState(windowId, windowOffset, windowLimit, batchId, taxYear, parentWorkflowId, newStatus, openingBalances, completedPositions, errorMessage);
    }

}