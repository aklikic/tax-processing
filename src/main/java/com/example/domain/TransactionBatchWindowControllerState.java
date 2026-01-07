package com.example.domain;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Domain model for BatchController workflow state.
 * Tracks the progress of coordinating multiple window workflows.
 */
public record TransactionBatchWindowControllerState(
    String batchId,
    String taxYear,
    ProcessingStatus status,
    long totalTransactions,
    int transactionsPerWindow,
    int windowCount,
    int maxParallelWindows,
    int nextWindowId,
    Map<String, WindowStatus> windowStatuses,
    long completedTransactions,
    String errorMessage
) {

    private static final Logger logger = LoggerFactory.getLogger(TransactionBatchWindowControllerState.class);

    public enum ProcessingStatus {
        PENDING, INITIALIZING, LAUNCHING_WINDOWS, AWAITING_WINDOW_SUB_WORKFLOWS_CALLBACK, COMPLETED, FAILED
    }

    private record BatchWindowWorkflowId(String batchId, String windowId){
        public String serialize() {
            return batchId + "-" + windowId;
        }
        public static BatchWindowWorkflowId deserialize(String raw) {
            var split = raw.split("-");
            return new BatchWindowWorkflowId(split[0], split[1]);
        }
    }

    public record WindowStatus(
        String windowId,
        int windowOffset,
        int windowLimit,
        String windowWorkflowId,
        WindowProcessingStatus status,
        long completedTransactions,
        String errorMessage
    ) {
        public static WindowStatus initializing(String batchId, String windowId, int windowOffset, int windowLimit) {
            var windowWorkflowId = new BatchWindowWorkflowId(batchId, windowId);
            return new WindowStatus(windowId, windowOffset, windowLimit, windowWorkflowId.serialize(), WindowProcessingStatus.TO_RUN, 0,null);
        }

        public WindowStatus withStatus(WindowProcessingStatus newStatus) {
            return new WindowStatus(windowId, windowOffset, windowLimit, windowWorkflowId, newStatus, completedTransactions, errorMessage);
        }
        public WindowStatus onResult(long windowCompletedTransactions, Optional<String> newErrorMessage) {
            return newErrorMessage.map(errMsg -> new WindowStatus(windowId, windowOffset, windowLimit, windowWorkflowId, WindowProcessingStatus.FAILED, completedTransactions+windowCompletedTransactions, errMsg))
                    .orElse(new WindowStatus(windowId, windowOffset, windowLimit, windowWorkflowId, WindowProcessingStatus.COMPLETED, completedTransactions+windowCompletedTransactions, errorMessage));
        }
    }

    public enum WindowProcessingStatus {
        TO_RUN, RUNNING, COMPLETED, FAILED
    }

    public static TransactionBatchWindowControllerState empty() {
        return new TransactionBatchWindowControllerState(
            "",
            "",
            ProcessingStatus.PENDING,
            0L,
            0,
            0,
            3, // Default max parallel windows
                0,
            new ConcurrentHashMap<>(),
            0,
            null
        );
    }

    public static TransactionBatchWindowControllerState initialize(String batchId, String taxYear, int maxParallelWindows, int transactionsPerWindow) {
        return new TransactionBatchWindowControllerState(
            batchId,
            taxYear,
            ProcessingStatus.INITIALIZING,
            0L,
            transactionsPerWindow,
            0,
            maxParallelWindows,
            0,
            new ConcurrentHashMap<>(),
            0,
            null
        );
    }

    public TransactionBatchWindowControllerState withTotalTransactions(long total) {
        return new TransactionBatchWindowControllerState(batchId, taxYear, status, total, transactionsPerWindow, windowCount, maxParallelWindows, nextWindowId, windowStatuses, completedTransactions, errorMessage);
    }

    public TransactionBatchWindowControllerState withWindowCount(int count) {
        return new TransactionBatchWindowControllerState(batchId, taxYear, status, totalTransactions, transactionsPerWindow,count, maxParallelWindows, nextWindowId, windowStatuses, completedTransactions, errorMessage);
    }

    public TransactionBatchWindowControllerState withStatus(ProcessingStatus newStatus) {
        return new TransactionBatchWindowControllerState(batchId, taxYear, newStatus, totalTransactions, transactionsPerWindow,windowCount, maxParallelWindows, nextWindowId, windowStatuses, completedTransactions, errorMessage);
    }


    public TransactionBatchWindowControllerState onWindowStatusResult(String windowId, long windowCompletedTransactions, Optional<String> newErrorMessage) {
        var updated = new ConcurrentHashMap<>(windowStatuses);
        updated.put(windowId, windowStatuses.get(windowId).onResult(windowCompletedTransactions, newErrorMessage));
        var newCompletedTransactions = completedTransactions + windowCompletedTransactions;
        return new TransactionBatchWindowControllerState(batchId, taxYear, status, totalTransactions, transactionsPerWindow, windowCount, maxParallelWindows, nextWindowId, updated, newCompletedTransactions , newErrorMessage.orElse(errorMessage));
    }


    public TransactionBatchWindowControllerState withError(String error) {
        return new TransactionBatchWindowControllerState(batchId, taxYear, ProcessingStatus.FAILED, totalTransactions, transactionsPerWindow, windowCount, maxParallelWindows, nextWindowId, windowStatuses, completedTransactions, error);
    }

    public TransactionBatchWindowControllerState prepareNextWindowBatchToLaunch() {
        var windowIdStartInclusive = nextWindowId;
        var windowIdEndExclusive = windowIdStartInclusive + maxParallelWindows;
        if(windowIdEndExclusive > windowCount) {
            windowIdEndExclusive = windowCount;
        }
        var newWindowStatuses =
                IntStream.range(windowIdStartInclusive, windowIdEndExclusive).mapToObj(index -> {
                    var windowId = index + "";
                    var windowOffset = index * transactionsPerWindow;
                    var windowLimit = transactionsPerWindow;
                    if(windowOffset + windowLimit > totalTransactions - 1) {
                        windowLimit = (int) (totalTransactions - windowOffset);
                    }

                    return TransactionBatchWindowControllerState.WindowStatus.initializing(batchId(), windowId, windowOffset, windowLimit);
                }).collect(Collectors.toMap(TransactionBatchWindowControllerState.WindowStatus::windowId, Function.identity()));

        var updated = new ConcurrentHashMap<>(windowStatuses);
        updated.putAll(newWindowStatuses);

        return new TransactionBatchWindowControllerState(batchId, taxYear, status, totalTransactions, transactionsPerWindow, windowCount, maxParallelWindows, windowIdEndExclusive ,updated, completedTransactions, errorMessage);

    }

    public List<WindowStatus> getWindowStatusesToRun() {
        return windowStatuses.values().stream()
                .filter(ws -> ws.status() == WindowProcessingStatus.TO_RUN)
                .collect(Collectors.toList());
    }

    public List<WindowStatus> getWindowStatusesRunning() {
        return windowStatuses.values().stream()
                .filter(ws -> ws.status() == WindowProcessingStatus.RUNNING)
                .collect(Collectors.toList());
    }

    public TransactionBatchWindowControllerState markWindowStatusesRunning(List<WindowStatus> windows){
        var updated = new ConcurrentHashMap<>(windowStatuses);
        for(WindowStatus windowStatus : windows){
            if(updated.get(windowStatus.windowId()).status() == WindowProcessingStatus.TO_RUN) {
                updated.put(windowStatus.windowId(), windowStatus.withStatus(WindowProcessingStatus.RUNNING));
            } else {
                logger.error("[{}] markWindowStatusesRunning status of {} is {}", batchId, windowStatus.windowId(), updated.get(windowStatus.windowId()).status());
            }
        }
        return new TransactionBatchWindowControllerState(batchId, taxYear, status, totalTransactions, transactionsPerWindow, windowCount, maxParallelWindows, nextWindowId,updated, completedTransactions, errorMessage);
    }
}