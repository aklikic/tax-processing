package com.example.domain;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Domain model for OpeningBalanceBatch workflow state.
 * Tracks the progress of coordinating multiple OpeningBalanceTransactionsBatch sub-workflows.
 */
public record BatchWindowState(
    String windowId,
    int windowOffset,
    int windowLimit,
    String batchId,
    String taxYear,
    String parentWorkflowId,
    ProcessingStatus status,
    List<OpeningBalance> openingBalances,
    int totalSubWorkflows,
    Map<String, SubWorkflowStatus> subWorkflowStates,
    String errorMessage
) {

    public enum ProcessingStatus {
        PENDING, LOADING_WINDOW, INITIALIZING_POSITIONS, LAUNCHING_TRANSACTION_PROCESSING, AWAITING_TRANSACTION_SUB_WORKFLOWS_CALLBACK, COMPLETED, FAILED
    }

    public record SubWorkflowStatus(
        String workflowId,
        OpeningBalanceTransactionsBatchState.ProcessingStatus status,
        int completedPositions,
        int totalPositions,
        String errorMessage
    ) {}

    public static BatchWindowState empty() {
        return new BatchWindowState(
            "",
          0,
            0,
            "",
            "",
            "",
            ProcessingStatus.PENDING,
            List.of(),
            0,
            new ConcurrentHashMap<>(),
            null
        );
    }

    public boolean isEmpty() {
        return windowId.isBlank();
    }

    public static BatchWindowState initialize(String windowId, int windowOffset, int windowLimit, String batchId, String taxYear, String parentWorkflowId) {
        return new BatchWindowState(
                windowId,
                windowOffset,
                windowLimit,
                batchId,
                taxYear,
                parentWorkflowId,
                ProcessingStatus.LOADING_WINDOW,
                List.of(),
                0,
                new ConcurrentHashMap<>(),
                null
        );
    }

    public BatchWindowState withOpeningBalances(List<OpeningBalance> balances){
        return new BatchWindowState(windowId, windowOffset, windowLimit, batchId, taxYear, parentWorkflowId, status, balances, totalSubWorkflows, subWorkflowStates, errorMessage);
    }

    public BatchWindowState withTotalSubWorkflows(int total){
        return new BatchWindowState(windowId, windowOffset, windowLimit, batchId, taxYear, parentWorkflowId, status, openingBalances, total, subWorkflowStates, errorMessage);
    }

    public BatchWindowState addSubWorkflow(SubWorkflowStatus subWorkflowStatus){
        var updatedStates = new ConcurrentHashMap<>(subWorkflowStates);
        updatedStates.put(subWorkflowStatus.workflowId, subWorkflowStatus);
        return new BatchWindowState(windowId, windowOffset, windowLimit, batchId, taxYear, parentWorkflowId, status,  openingBalances, totalSubWorkflows, updatedStates, errorMessage);
    }

    public BatchWindowState withStatus(ProcessingStatus status) {
        return new BatchWindowState(windowId, windowOffset, windowLimit, batchId, taxYear, parentWorkflowId, status, openingBalances, totalSubWorkflows, subWorkflowStates, errorMessage);
    }

    public BatchWindowState withError(String error) {
        return new BatchWindowState(windowId, windowOffset, windowLimit, batchId, taxYear, parentWorkflowId, status, openingBalances, totalSubWorkflows, subWorkflowStates, errorMessage);
    }


    public long getCompletedPositionsCount(){
        return subWorkflowStates.values().stream().filter(sw -> sw.status() == OpeningBalanceTransactionsBatchState.ProcessingStatus.COMPLETED).count();
    }

}