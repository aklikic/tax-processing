package com.example.domain;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for PositionBatchControllerState domain logic.
 */
public class PositionBatchControllerStateTest {

    @Test
    public void shouldRemoveOldCompletedWindowsWhenExceedingLimit() {
        // Create initial state with multiple windows
        var windowStatuses = new ConcurrentHashMap<String, PositionBatchControllerState.WindowStatus>();

        // Add completed windows (window IDs 0-4, where higher ID = newer)
        windowStatuses.put("0", new PositionBatchControllerState.WindowStatus("0", 0, 100, "batch-0", PositionBatchControllerState.WindowProcessingStatus.COMPLETED, 100, null));
        windowStatuses.put("1", new PositionBatchControllerState.WindowStatus("1", 100, 100, "batch-1", PositionBatchControllerState.WindowProcessingStatus.COMPLETED, 100, null));
        windowStatuses.put("2", new PositionBatchControllerState.WindowStatus("2", 200, 100, "batch-2", PositionBatchControllerState.WindowProcessingStatus.COMPLETED, 100, null));
        windowStatuses.put("3", new PositionBatchControllerState.WindowStatus("3", 300, 100, "batch-3", PositionBatchControllerState.WindowProcessingStatus.FAILED, 50, "Error"));
        windowStatuses.put("4", new PositionBatchControllerState.WindowStatus("4", 400, 100, "batch-4", PositionBatchControllerState.WindowProcessingStatus.RUNNING, 0, null));

        var state = new PositionBatchControllerState(
            "batch-001", "2023", PositionBatchControllerState.ProcessingStatus.LAUNCHING_WINDOWS,
            1000L, 100, 10, 3, 5, windowStatuses, 350L, 3, null
        );

        // Add window 5 as running first, then complete it
        windowStatuses.put("5", new PositionBatchControllerState.WindowStatus("5", 500, 100, "batch-5", PositionBatchControllerState.WindowProcessingStatus.RUNNING, 0, null));
        state = new PositionBatchControllerState(
            "batch-001", "2023", PositionBatchControllerState.ProcessingStatus.LAUNCHING_WINDOWS,
            1000L, 100, 10, 3, 5, windowStatuses, 350L, 3, null
        );

        // Complete window 5, with limit of 2 completed/failed windows to keep
        var newState = state.onWindowStatusResult("5", 100, Optional.empty(), 2);

        // Should keep only the 2 newest completed/failed windows: 5 (just completed) and 3 (failed)
        // Should remove older completed windows: 0, 1, 2
        // Should keep running window: 4
        assertThat(newState.windowStatuses()).hasSize(3);
        assertThat(newState.windowStatuses()).containsOnlyKeys("3", "4", "5");

        // Verify the kept windows have correct statuses
        assertThat(newState.windowStatuses().get("3").status()).isEqualTo(PositionBatchControllerState.WindowProcessingStatus.FAILED);
        assertThat(newState.windowStatuses().get("4").status()).isEqualTo(PositionBatchControllerState.WindowProcessingStatus.RUNNING);
        assertThat(newState.windowStatuses().get("5").status()).isEqualTo(PositionBatchControllerState.WindowProcessingStatus.COMPLETED);

        // Verify completed positions updated
        assertThat(newState.completedPositions()).isEqualTo(450); // 350 + 100
    }

    @Test
    public void shouldNotRemoveWindowsWhenUnderLimit() {
        var windowStatuses = new ConcurrentHashMap<String, PositionBatchControllerState.WindowStatus>();

        // Add only 2 completed windows
        windowStatuses.put("0", new PositionBatchControllerState.WindowStatus("0", 0, 100, "batch-0", PositionBatchControllerState.WindowProcessingStatus.COMPLETED, 100, null));
        windowStatuses.put("1", new PositionBatchControllerState.WindowStatus("1", 100, 100, "batch-1", PositionBatchControllerState.WindowProcessingStatus.RUNNING, 0, null));

        var state = new PositionBatchControllerState(
            "batch-001", "2023", PositionBatchControllerState.ProcessingStatus.LAUNCHING_WINDOWS,
            1000L, 100, 10, 3, 2, windowStatuses, 100L, 1, null
        );

        // Complete window 1, with limit of 3 completed/failed windows to keep
        var newState = state.onWindowStatusResult("1", 100, Optional.empty(), 3);

        // Should keep all windows since we're under the limit
        assertThat(newState.windowStatuses()).hasSize(2);
        assertThat(newState.windowStatuses()).containsOnlyKeys("0", "1");
        assertThat(newState.windowStatuses().get("1").status()).isEqualTo(PositionBatchControllerState.WindowProcessingStatus.COMPLETED);
    }

    @Test
    public void shouldHandleExactlyAtLimit() {
        var windowStatuses = new ConcurrentHashMap<String, PositionBatchControllerState.WindowStatus>();

        // Add exactly 2 completed windows
        windowStatuses.put("0", new PositionBatchControllerState.WindowStatus("0", 0, 100, "batch-0", PositionBatchControllerState.WindowProcessingStatus.COMPLETED, 100, null));
        windowStatuses.put("1", new PositionBatchControllerState.WindowStatus("1", 100, 100, "batch-1", PositionBatchControllerState.WindowProcessingStatus.FAILED, 50, "Error"));
        windowStatuses.put("2", new PositionBatchControllerState.WindowStatus("2", 200, 100, "batch-2", PositionBatchControllerState.WindowProcessingStatus.RUNNING, 0, null));

        var state = new PositionBatchControllerState(
            "batch-001", "2023", PositionBatchControllerState.ProcessingStatus.LAUNCHING_WINDOWS,
            1000L, 100, 10, 3, 2, windowStatuses, 150L, 2, null
        );

        // Complete window 2, with limit of 2 completed/failed windows to keep
        var newState = state.onWindowStatusResult("2", 100, Optional.empty(), 2);

        // Should keep the 2 newest: 2 (just completed) and 1 (failed)
        // Should remove oldest: 0
        assertThat(newState.windowStatuses()).hasSize(2);
        assertThat(newState.windowStatuses()).containsOnlyKeys("1", "2");
    }

    @Test
    public void shouldNotRemoveRunningOrToRunWindows() {
        var windowStatuses = new ConcurrentHashMap<String, PositionBatchControllerState.WindowStatus>();

        // Mix of all statuses
        windowStatuses.put("0", new PositionBatchControllerState.WindowStatus("0", 0, 100, "batch-0", PositionBatchControllerState.WindowProcessingStatus.COMPLETED, 100, null));
        windowStatuses.put("1", new PositionBatchControllerState.WindowStatus("1", 100, 100, "batch-1", PositionBatchControllerState.WindowProcessingStatus.RUNNING, 0, null));
        windowStatuses.put("2", new PositionBatchControllerState.WindowStatus("2", 200, 100, "batch-2", PositionBatchControllerState.WindowProcessingStatus.TO_RUN, 0, null));
        windowStatuses.put("3", new PositionBatchControllerState.WindowStatus("3", 300, 100, "batch-3", PositionBatchControllerState.WindowProcessingStatus.COMPLETED, 100, null));
        windowStatuses.put("4", new PositionBatchControllerState.WindowStatus("4", 400, 100, "batch-4", PositionBatchControllerState.WindowProcessingStatus.RUNNING, 0, null));

        var state = new PositionBatchControllerState(
            "batch-001", "2023", PositionBatchControllerState.ProcessingStatus.LAUNCHING_WINDOWS,
            1000L, 100, 10, 3, 4, windowStatuses, 200L, 2, null
        );

        // Complete window 4, with limit of 1 completed/failed window to keep
        var newState = state.onWindowStatusResult("4", 100, Optional.empty(), 1);

        // Should keep: 4 (newest completed), 1 (running), 2 (to_run)
        // Should remove: 0, 3 (older completed)
        assertThat(newState.windowStatuses()).hasSize(3);
        assertThat(newState.windowStatuses()).containsOnlyKeys("1", "2", "4");

        // Verify statuses
        assertThat(newState.windowStatuses().get("1").status()).isEqualTo(PositionBatchControllerState.WindowProcessingStatus.RUNNING);
        assertThat(newState.windowStatuses().get("2").status()).isEqualTo(PositionBatchControllerState.WindowProcessingStatus.TO_RUN);
        assertThat(newState.windowStatuses().get("4").status()).isEqualTo(PositionBatchControllerState.WindowProcessingStatus.COMPLETED);
    }

    @Test
    public void shouldNotLaunchNewWindowsWhenAtMaxParallelLimit() {
        var windowStatuses = new ConcurrentHashMap<String, PositionBatchControllerState.WindowStatus>();

        // Create scenario where we're at the maxParallelWindows limit
        windowStatuses.put("0", new PositionBatchControllerState.WindowStatus("0", 0, 100, "batch-0", PositionBatchControllerState.WindowProcessingStatus.RUNNING, 0, null));
        windowStatuses.put("1", new PositionBatchControllerState.WindowStatus("1", 100, 100, "batch-1", PositionBatchControllerState.WindowProcessingStatus.RUNNING, 0, null));
        windowStatuses.put("2", new PositionBatchControllerState.WindowStatus("2", 200, 100, "batch-2", PositionBatchControllerState.WindowProcessingStatus.RUNNING, 0, null));

        var state = new PositionBatchControllerState(
            "batch-001", "2023", PositionBatchControllerState.ProcessingStatus.LAUNCHING_WINDOWS,
            2000L, 100, 20, 3, 10, windowStatuses, 300L, 3, null  // maxParallelWindows=3 and exactly 3 windows running
        );

        // Try to prepare next batch - should not create any new windows
        var newState = state.prepareNextWindowBatchToLaunch();

        // nextWindowId should stay the same since no new windows created
        assertThat(newState.nextWindowId()).isEqualTo(10);

        // Should not create any new windows since we're at the limit
        assertThat(newState.windowStatuses()).hasSize(3); // Same size as before
        assertThat(newState.windowStatuses()).containsOnlyKeys("0", "1", "2"); // Same windows
    }

    @Test
    public void shouldLaunchOnlyAvailableSlotsWhenUnderMaxParallelLimit() {
        var windowStatuses = new ConcurrentHashMap<String, PositionBatchControllerState.WindowStatus>();

        // Create scenario where we have 2 running windows but max is 5
        windowStatuses.put("0", new PositionBatchControllerState.WindowStatus("0", 0, 100, "batch-0", PositionBatchControllerState.WindowProcessingStatus.RUNNING, 0, null));
        windowStatuses.put("1", new PositionBatchControllerState.WindowStatus("1", 100, 100, "batch-1", PositionBatchControllerState.WindowProcessingStatus.RUNNING, 0, null));

        var state = new PositionBatchControllerState(
            "batch-001", "2023", PositionBatchControllerState.ProcessingStatus.LAUNCHING_WINDOWS,
            2000L, 100, 20, 5, 10, windowStatuses, 200L, 2, null  // maxParallelWindows=5, 2 running, nextWindowId=10
        );

        // Try to prepare next batch - should create exactly 3 new windows (5 max - 2 running = 3 available)
        var newState = state.prepareNextWindowBatchToLaunch();

        // nextWindowId should advance by 3
        assertThat(newState.nextWindowId()).isEqualTo(13);

        // Should create 3 new windows (10, 11, 12)
        assertThat(newState.windowStatuses()).hasSize(5);
        assertThat(newState.windowStatuses()).containsKeys("0", "1", "10", "11", "12");

        // New windows should have TO_RUN status
        assertThat(newState.windowStatuses().get("10").status()).isEqualTo(PositionBatchControllerState.WindowProcessingStatus.TO_RUN);
        assertThat(newState.windowStatuses().get("11").status()).isEqualTo(PositionBatchControllerState.WindowProcessingStatus.TO_RUN);
        assertThat(newState.windowStatuses().get("12").status()).isEqualTo(PositionBatchControllerState.WindowProcessingStatus.TO_RUN);
    }
}