package com.example.domain;

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
public record BatchControllerState(
    String batchId,
    String taxYear,
    ProcessingStatus status,
    long totalPositions,
    int positionsPerWindow,
    int totalWindows,
    int maxParallelWindows,
    int lastWindowId,
    Map<String, WindowStatus> windowStatuses,
    long completedPositions,
    String errorMessage
) {

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
        long completedPositions,
        String errorMessage
    ) {
        public static WindowStatus initializing(String batchId, String windowId, int windowOffset, int windowLimit) {
            var windowWorkflowId = new BatchWindowWorkflowId(batchId, windowId);
            return new WindowStatus(windowId, windowOffset, windowLimit, windowWorkflowId.serialize(),WindowProcessingStatus.RUNNING, 0,null);
        }

        public WindowStatus withStatus(WindowProcessingStatus newStatus) {
            return new WindowStatus(windowId, windowOffset, windowLimit, windowWorkflowId, newStatus, completedPositions, errorMessage);
        }
        public WindowStatus onResult(long windowCompletedPositions, Optional<String> newErrorMessage) {
            return newErrorMessage.map(errMsg -> new WindowStatus(windowId, windowOffset, windowLimit, windowWorkflowId, WindowProcessingStatus.FAILED, windowCompletedPositions, errMsg))
                    .orElse(new WindowStatus(windowId, windowOffset, windowLimit, windowWorkflowId, WindowProcessingStatus.COMPLETED, windowCompletedPositions, errorMessage));
        }
    }

    public enum WindowProcessingStatus {
        RUNNING, COMPLETED, FAILED
    }

    public static BatchControllerState empty() {
        return new BatchControllerState(
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

    public static BatchControllerState initialize(String batchId, String taxYear, int maxParallelWindows, int positionsPerWindow) {
        return new BatchControllerState(
            batchId,
            taxYear,
            ProcessingStatus.INITIALIZING,
            0L,
            positionsPerWindow,
            0,
            maxParallelWindows,
            0,
            new ConcurrentHashMap<>(),
            0,
            null
        );
    }

    public BatchControllerState withTotalPositions(long total) {
        return new BatchControllerState(batchId, taxYear, status, total, positionsPerWindow, totalWindows, maxParallelWindows, lastWindowId, windowStatuses, completedPositions, errorMessage);
    }

    public BatchControllerState withTotalWindows(int total) {
        return new BatchControllerState(batchId, taxYear, status, totalPositions, positionsPerWindow,total, maxParallelWindows, lastWindowId, windowStatuses, completedPositions, errorMessage);
    }

    public BatchControllerState withStatus(ProcessingStatus newStatus) {
        return new BatchControllerState(batchId, taxYear, newStatus, totalPositions, positionsPerWindow,totalWindows, maxParallelWindows, lastWindowId, windowStatuses, completedPositions, errorMessage);
    }


    public BatchControllerState onWindowStatusResult(String windowId, long windowCompletedPositions, Optional<String> newErrorMessage) {
        var updated = new ConcurrentHashMap<>(windowStatuses);
        updated.put(windowId, windowStatuses.get(windowId).onResult(windowCompletedPositions, newErrorMessage));
        var newCompletedPositions = completedPositions + windowCompletedPositions;
        return new BatchControllerState(batchId, taxYear, status, totalPositions, positionsPerWindow,totalWindows, maxParallelWindows, lastWindowId, updated, newCompletedPositions , newErrorMessage.orElse(errorMessage));
    }


    public BatchControllerState withError(String error) {
        return new BatchControllerState(batchId, taxYear, ProcessingStatus.FAILED, totalPositions, positionsPerWindow, totalWindows, maxParallelWindows, lastWindowId, windowStatuses, completedPositions, error);
    }

    public BatchControllerState prepareNextWindowBatchToLaunch() {
        var windowIdStartInclusive = lastWindowId;
        var windowIdEndExclusive = windowIdStartInclusive + maxParallelWindows;
        if(windowIdEndExclusive > totalWindows) {
            windowIdEndExclusive = totalWindows;
        }
        var windowStatuses =
                IntStream.range(windowIdStartInclusive, windowIdEndExclusive).mapToObj(index -> {
                    var windowId = index + "";
                    var windowOffset = index * positionsPerWindow;
                    var windowLimit = positionsPerWindow;
                    if(windowOffset + windowLimit > totalPositions - 1) {
                        windowLimit = (int) (totalPositions - windowOffset);
                    }

                    return BatchControllerState.WindowStatus.initializing(batchId(), windowId, windowOffset, windowLimit);
                }).collect(Collectors.toMap(BatchControllerState.WindowStatus::windowId, Function.identity()));

        var updated = new ConcurrentHashMap<>(windowStatuses);
        updated.putAll(windowStatuses);

        return new BatchControllerState(batchId, taxYear, status, totalPositions, positionsPerWindow, totalWindows, maxParallelWindows, windowIdEndExclusive ,updated, completedPositions, errorMessage);

    }

    public List<WindowStatus> getWindowStatusesToRun() {
        return windowStatuses.values().stream()
                .filter(ws -> ws.status() == WindowProcessingStatus.RUNNING)
                .collect(Collectors.toList());
    }

//    public int waitingWindowsCount() {
//        return (int) windowStatuses.values().stream()
//                .filter(ws -> ws.status() == WindowProcessingStatus.WAITING)
//                .count();
//    }

    public int runningWindowsCount() {
        return (int) windowStatuses.values().stream()
            .filter(ws -> ws.status() == WindowProcessingStatus.RUNNING)
            .count();
    }

    public int completedWindowsCount() {
        return (int) windowStatuses.values().stream()
            .filter(ws -> ws.status() == WindowProcessingStatus.COMPLETED)
            .count();
    }

    public boolean allWindowsCompleted() {
        return completedWindowsCount() == totalWindows;
    }

    public boolean hasFailedWindows() {
        return windowStatuses.values().stream()
            .anyMatch(ws -> ws.status() == WindowProcessingStatus.FAILED);
    }
}