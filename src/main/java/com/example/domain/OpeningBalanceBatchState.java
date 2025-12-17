package com.example.domain;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Domain model for OpeningBalanceBatch workflow state.
 * Tracks the progress of coordinating multiple OpeningBalanceTransactionsBatch sub-workflows.
 */
public record OpeningBalanceBatchState(
    String batchId,
    String taxYear,
    ProcessingStatus status,
    long totalPositions,
//    int totalSubWorkflows,
//    int completedSubWorkflows,
    int currentWindow,
    int totalWindows,
//    Map<String, SubWorkflowStatus> subWorkflowStates,
    Map<Integer, Window> windows,
    String errorMessage
) {

    public enum ProcessingStatus {
        PENDING, INITIALIZING, LOAD_NEXT_WINDOW, INITIALIZING_POSITIONS, LAUNCHING_TRANSACTION_PROCESSING, AWAITING_TRANSACTION_SUB_WORKFLOWS_CALLBACK, COMPLETED, FAILED
    }

    public record Window(int windowId,  List<OpeningBalance> openingBalances, int totalSubWorkflows, Map<String, SubWorkflowStatus> subWorkflowStates) {
        @Override
        public boolean equals(Object obj) {
            return obj instanceof Window && ((Window)obj).windowId == windowId;
        }

        public static Window init(int windowId){
            return new Window(windowId,List.of(), 0,  new ConcurrentHashMap<>());
        }

        public Window withOpeningBalances(List<OpeningBalance> balances){
            return new Window(windowId,balances, totalSubWorkflows, subWorkflowStates);
        }

        public Window withTotalSubWorkflows(int total){
            return new Window(windowId,openingBalances, total, subWorkflowStates);
        }

        public Window addSubWorkflow(SubWorkflowStatus subWorkflowStatus){
            var updatedStates = new ConcurrentHashMap<>(subWorkflowStates);
            updatedStates.put(subWorkflowStatus.workflowId, subWorkflowStatus);
            return new Window(windowId,openingBalances, totalSubWorkflows, updatedStates);
        }


    }

    public record SubWorkflowStatus(
        String workflowId,
        OpeningBalanceTransactionsBatchState.ProcessingStatus status,
        int completedPositions,
        int totalPositions,
        String errorMessage
    ) {}

    public static OpeningBalanceBatchState empty() {
        return new OpeningBalanceBatchState(
            "",
            "",
            ProcessingStatus.PENDING,
            0L,
            0,
            0,
            new ConcurrentHashMap<>(),
            null
        );
    }

    public static OpeningBalanceBatchState initialize(String batchId, String taxYear) {
        return new OpeningBalanceBatchState(
                batchId,
                taxYear,
                ProcessingStatus.INITIALIZING,
                0L,
                0,
                0,
                new ConcurrentHashMap<>(),
                null
        );
    }

    public OpeningBalanceBatchState withTotalPositions(long total) {
        return new OpeningBalanceBatchState(batchId, taxYear, status, total,  currentWindow, totalWindows, windows, errorMessage);
    }

    public OpeningBalanceBatchState withTotalWindow(int total) {
        return new OpeningBalanceBatchState(batchId, taxYear, status, totalPositions, currentWindow,  total, windows,errorMessage); // Clear balances for new window
    }

    public OpeningBalanceBatchState withCurrentWindow(int windowId) {
        var updatedWindows = new ConcurrentHashMap<>(windows);
        updatedWindows.put(windowId, Window.init(windowId));
        return new OpeningBalanceBatchState(batchId, taxYear, status, totalPositions, windowId, totalWindows, updatedWindows, errorMessage); // Clear balances for new window
    }

    public OpeningBalanceBatchState withOpeningBalancesForCurrentWindow(int windowId, List<OpeningBalance> balances) {
        var updatedWindows = new ConcurrentHashMap<>(windows);
        updatedWindows.put(windowId, windows.get(windowId).withOpeningBalances(balances));
        return new OpeningBalanceBatchState(batchId, taxYear, status, totalPositions, currentWindow, totalWindows, updatedWindows, errorMessage);


    }

    public OpeningBalanceBatchState withTotalSubWorkflows(int windowId, int total) {
        var updatedWindows = new ConcurrentHashMap<>(windows);
        updatedWindows.put(windowId, windows.get(windowId).withTotalSubWorkflows(total));
        return new OpeningBalanceBatchState(batchId, taxYear, status, totalPositions, currentWindow, totalWindows, updatedWindows, errorMessage);

    }

    public OpeningBalanceBatchState addSubWorkflowState(int windowId, SubWorkflowStatus subStatus) {
        var updatedWindows = new ConcurrentHashMap<>(windows);
        updatedWindows.put(windowId, windows.get(windowId).addSubWorkflow(subStatus));
        return new OpeningBalanceBatchState(batchId, taxYear, status, totalPositions, currentWindow, totalWindows, updatedWindows, errorMessage);
    }

    public OpeningBalanceBatchState withStatus(ProcessingStatus status) {
        return new OpeningBalanceBatchState(batchId, taxYear, status, totalPositions, currentWindow, totalWindows, windows, errorMessage);
    }

    public OpeningBalanceBatchState withError(String error) {
        return new OpeningBalanceBatchState(batchId, taxYear, ProcessingStatus.FAILED, totalPositions,  currentWindow, totalWindows, windows, error);
    }

//    public double progress() {
//        if (totalSubWorkflows == 0) return 0.0;
//        return (double) completedSubWorkflows / totalSubWorkflows * 100.0;
//    }

}