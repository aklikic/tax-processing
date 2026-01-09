package com.example.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
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
public record PositionBatchControllerState(
    String batchId,
    String taxYear,
    ProcessingStatus status,
    long totalPositions,
    int positionsPerWindow,
    int totalWindows,
    int maxParallelWindows,
    int nextWindowId,
    Map<String, WindowStatus> windowStatuses,
    long completedPositions,
    int completedWindows,
    String errorMessage
) {

    private static final Logger logger = LoggerFactory.getLogger(PositionBatchControllerState.class);

    public enum ProcessingStatus {
        PENDING, INITIALIZING, LAUNCHING_WINDOWS, AWAITING_WINDOW_SUB_WORKFLOWS_CALLBACK, COMPLETED, FAILED
    }

    public record BatchWindowWorkflowId(String batchId, String windowId){
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
            return new WindowStatus(windowId, windowOffset, windowLimit, windowWorkflowId.serialize(), WindowProcessingStatus.TO_RUN, 0,null);
        }

        public WindowStatus withStatus(WindowProcessingStatus newStatus) {
            return new WindowStatus(windowId, windowOffset, windowLimit, windowWorkflowId, newStatus, completedPositions, errorMessage);
        }
        public WindowStatus onResult(long windowCompletedPositions, Optional<String> newErrorMessage) {
            return newErrorMessage.map(errMsg -> new WindowStatus(windowId, windowOffset, windowLimit, windowWorkflowId, WindowProcessingStatus.FAILED, windowCompletedPositions, errMsg))
                    .orElse(new WindowStatus(windowId, windowOffset, windowLimit, windowWorkflowId, WindowProcessingStatus.COMPLETED, completedPositions+windowCompletedPositions, errorMessage));
        }
    }

    public enum WindowProcessingStatus {
        TO_RUN, RUNNING, COMPLETED, FAILED
    }

    public static PositionBatchControllerState empty() {
        return new PositionBatchControllerState(
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
            0,
            null
        );
    }

    public static PositionBatchControllerState initialize(String batchId, String taxYear, int maxParallelWindows, int positionsPerWindow) {
        return new PositionBatchControllerState(
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
            0,
            null
        );
    }

    public PositionBatchControllerState withTotalPositions(long total) {
        return new PositionBatchControllerState(batchId, taxYear, status, total, positionsPerWindow, totalWindows, maxParallelWindows, nextWindowId, windowStatuses, completedPositions, completedWindows, errorMessage);
    }

    public PositionBatchControllerState withTotalWindows(int total) {
        return new PositionBatchControllerState(batchId, taxYear, status, totalPositions, positionsPerWindow,total, maxParallelWindows, nextWindowId, windowStatuses, completedPositions, completedWindows, errorMessage);
    }

    public PositionBatchControllerState withStatus(ProcessingStatus newStatus) {
        return new PositionBatchControllerState(batchId, taxYear, newStatus, totalPositions, positionsPerWindow,totalWindows, maxParallelWindows, nextWindowId, windowStatuses, completedPositions, completedWindows, errorMessage);
    }


    public PositionBatchControllerState onWindowStatusResult(String windowId, long windowCompletedPositions, Optional<String> newErrorMessage, int maxCompletedWindowsToKeepInState) {
        var updated = new ConcurrentHashMap<>(windowStatuses);
        updated.put(windowId, windowStatuses.get(windowId).onResult(windowCompletedPositions, newErrorMessage));

        // Remove old completed/failed entries, keeping only the newest ones up to the limit
        var completedOrFailed = updated.values().stream()
                .filter(ws -> ws.status() == WindowProcessingStatus.COMPLETED || ws.status() == WindowProcessingStatus.FAILED)
                .sorted((w1, w2) -> Integer.compare(Integer.parseInt(w2.windowId()), Integer.parseInt(w1.windowId()))) // Sort by windowId descending (newest first)
                .collect(Collectors.toList());

        if (completedOrFailed.size() > maxCompletedWindowsToKeepInState) {
            var toRemove = completedOrFailed.subList(maxCompletedWindowsToKeepInState, completedOrFailed.size());
            toRemove.forEach(ws -> updated.remove(ws.windowId()));
        }

        var newCompletedPositions = completedPositions + windowCompletedPositions;
        var newCompletedWindows = completedWindows + 1;
        return new PositionBatchControllerState(batchId, taxYear, status, totalPositions, positionsPerWindow,totalWindows, maxParallelWindows, nextWindowId, updated, newCompletedPositions, newCompletedWindows, newErrorMessage.orElse(errorMessage));
    }


    public PositionBatchControllerState withError(String error) {
        return new PositionBatchControllerState(batchId, taxYear, ProcessingStatus.FAILED, totalPositions, positionsPerWindow, totalWindows, maxParallelWindows, nextWindowId, windowStatuses, completedPositions, completedWindows, error);
    }

    public PositionBatchControllerState prepareNextWindowBatchToLaunch() {
        var windowIdStartInclusive = nextWindowId;
        var alreadyRunning = getWindowStatusesRunning().size();

        // Calculate how many new windows we can launch without exceeding maxParallelWindows
        var availableSlots = maxParallelWindows - alreadyRunning;

        logger.debug("[{}] prepareNextWindowBatchToLaunch: nextWindowId={}, alreadyRunning={}, maxParallelWindows={}, availableSlots={}",
                batchId, nextWindowId, alreadyRunning, maxParallelWindows, availableSlots);

        // If no slots available, don't create any new windows
        if (availableSlots <= 0) {
            logger.debug("[{}] prepareNextWindowBatchToLaunch: No available slots (running={}, max={}). Skipping window creation.",
                    batchId, alreadyRunning, maxParallelWindows);
            return new PositionBatchControllerState(batchId, taxYear, status, totalPositions, positionsPerWindow, totalWindows, maxParallelWindows, nextWindowId, windowStatuses, completedPositions, completedWindows, errorMessage);
        }

        // Only create windows up to the available slots
        var windowIdEndExclusive = windowIdStartInclusive + availableSlots;

        if(windowIdEndExclusive > totalWindows) {
            windowIdEndExclusive = totalWindows;
        }
        var newWindowStatuses =
                IntStream.range(windowIdStartInclusive, windowIdEndExclusive).mapToObj(index -> {
                    var windowId = index + "";
                    var windowOffset = index * positionsPerWindow;
                    var windowLimit = positionsPerWindow;
                    if(windowOffset + windowLimit > totalPositions - 1) {
                        windowLimit = (int) (totalPositions - windowOffset);
                    }

                    return PositionBatchControllerState.WindowStatus.initializing(batchId(), windowId, windowOffset, windowLimit);
                }).collect(Collectors.toMap(PositionBatchControllerState.WindowStatus::windowId, Function.identity()));

        var updated = new ConcurrentHashMap<>(windowStatuses);
        updated.putAll(newWindowStatuses);

        return new PositionBatchControllerState(batchId, taxYear, status, totalPositions, positionsPerWindow, totalWindows, maxParallelWindows, windowIdEndExclusive ,updated, completedPositions, completedWindows, errorMessage);

    }

    @JsonIgnore
    public List<WindowStatus> getWindowStatusesToRun() {
        return windowStatuses.values().stream()
                .filter(ws -> ws.status() == WindowProcessingStatus.TO_RUN)
                .collect(Collectors.toList());
    }
    @JsonIgnore
    public List<WindowStatus> getWindowStatusesRunning() {
        return windowStatuses.values().stream()
                .filter(ws -> ws.status() == WindowProcessingStatus.RUNNING)
                .collect(Collectors.toList());
    }

    public PositionBatchControllerState markWindowStatusesRunning(List<WindowStatus> windows){
        var updated = new ConcurrentHashMap<>(windowStatuses);
        for(WindowStatus windowStatus : windows){
            if(updated.get(windowStatus.windowId()).status() == WindowProcessingStatus.TO_RUN) {
                updated.put(windowStatus.windowId(), windowStatus.withStatus(WindowProcessingStatus.RUNNING));
            } else {
                logger.error("[{}] markWindowStatusesRunning status of {} is {}", batchId, windowStatus.windowId(), updated.get(windowStatus.windowId()).status());
            }
        }
        return new PositionBatchControllerState(batchId, taxYear, status, totalPositions, positionsPerWindow, totalWindows, maxParallelWindows, nextWindowId,updated, completedPositions, completedWindows, errorMessage);
    }
}