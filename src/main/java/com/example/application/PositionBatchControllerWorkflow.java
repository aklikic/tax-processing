package com.example.application;

import akka.Done;
import akka.javasdk.annotations.Component;
import akka.javasdk.annotations.StepName;
import akka.javasdk.client.ComponentClient;
import akka.javasdk.workflow.Workflow;
import com.example.domain.PositionBatchControllerState;
import com.example.domain.ProcessingConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;


/**
 * Main workflow orchestrating the complete opening balance batch processing.
 * Processes opening balance windows sequentially with callback-based sub-workflows.
 */
@Component(id = "position-batch-controller")
public class PositionBatchControllerWorkflow extends Workflow<PositionBatchControllerState> {

    private static final Logger logger = LoggerFactory.getLogger(PositionBatchControllerWorkflow.class);

    private final ComponentClient componentClient;
    private final TaxDataRepository taxDataRepository;
    private final ProcessingConfig processingConfig;

    public PositionBatchControllerWorkflow(
        ComponentClient componentClient,
        TaxDataRepository taxDataRepository,
        ProcessingConfig processingConfig
    ) {
        this.componentClient = componentClient;
        this.taxDataRepository = taxDataRepository;
        this.processingConfig = processingConfig;
    }

    @Override
    public WorkflowSettings settings() {
        return WorkflowSettings.builder()
            .stepTimeout(PositionBatchControllerWorkflow::initializationStep, Duration.ofSeconds(30))
            .stepTimeout(PositionBatchControllerWorkflow::launchWindowsStep, Duration.ofSeconds(60))
            .defaultStepRecovery(maxRetries(2).failoverTo(PositionBatchControllerWorkflow::errorHandlingStep))
            .build();
    }

    public record StartBatchCommand(String batchId, String taxYear) {}

    public record BatchStatusResponse(
        String batchId,
        String taxYear,
        PositionBatchControllerState.ProcessingStatus status,
        long totalPositions,
        int totalWindows,
        int runningWindows,
        int completedWindows,
        long completedPositions,
        String errorMessage
    ) {}

    public record SubWorkflowCompletedCommand(
        String windowId,
        long windowCompletedPositions,
        String errorMessage
    ) {}

    private record OpeningBalanceTransactionsBatchWorkflowId(String batchId, int window, int positionBatchIndex){
        public String serialize() {
            return batchId + "-" + window + "-" + positionBatchIndex;
        }
        public static OpeningBalanceTransactionsBatchWorkflowId deserialize(String raw) {
            var split = raw.split("-");
            return new OpeningBalanceTransactionsBatchWorkflowId(split[0], Integer.parseInt(split[1]), Integer.parseInt(split[2]));
        }
    }

    @Override
    public PositionBatchControllerState emptyState() {
        return PositionBatchControllerState.empty();
    }

    /**
     * Start the complete opening balance batch processing.
     */
    public Effect<Done> start(StartBatchCommand command) {
        logger.info("Starting OpeningBalanceBatchWorkflow: {}", commandContext().workflowId());

        return effects()
            .updateState(PositionBatchControllerState.initialize(command.batchId(), command.taxYear(), processingConfig.positionMaxParallelWindows(), processingConfig.positionNumberPerWindow()))
            .transitionTo(PositionBatchControllerWorkflow::initializationStep)
            .thenReply(Done.getInstance());
    }

    /**
     * Get current batch processing status.
     */
    public Effect<BatchStatusResponse> getStatus() {
        var state = currentState();
        return effects().reply(new BatchStatusResponse(
            state.batchId(),
            state.taxYear(),
            state.status(),
            state.totalPositions(),
            state.totalWindows(),
            state.getWindowStatusesRunning().size(),
            state.completedWindows(),
            state.completedPositions(),
            state.errorMessage()
        ));
    }

    /**
     * Callback method for sub-workflows to report completion.
     */
    public Effect<Done> onSubWorkflowCompleted(SubWorkflowCompletedCommand command) {
        var state = currentState();

        if (state.status() == PositionBatchControllerState.ProcessingStatus.COMPLETED ||
                (state.windowStatuses().containsKey(command.windowId()) && state.windowStatuses().get(command.windowId()).status() != PositionBatchControllerState.WindowProcessingStatus.RUNNING)) {
            logger.info("[{}] Ignoring late sub-workflow completion notification for {}",
                    commandContext().workflowId(), command.windowId());
            return effects().reply(Done.getInstance());
        }

        logger.info("[{}] Callback for sub-workflow {} received", commandContext().workflowId(), command.windowId());

        // Add completed sub-workflow to state
        var updatedState = state.onWindowStatusResult(command.windowId(), command.windowCompletedPositions(), Optional.ofNullable(command.errorMessage), processingConfig.positionsMaxCompletedWindowsToKeepInState());

        // prepare next window batch to launch
        updatedState = updatedState.prepareNextWindowBatchToLaunch();

        var windowStatusesRunningCount = updatedState.getWindowStatusesRunning().size();
        var windowStatusesNextToRun = updatedState.getWindowStatusesToRun().size();

        if(windowStatusesRunningCount == 0 && windowStatusesNextToRun == 0){
            logger.info("[{}] All sub-workflows completed successfully. Not more windows to run. DONE!",
                    commandContext().workflowId());
            return effects()
                    .updateState(updatedState.withStatus(PositionBatchControllerState.ProcessingStatus.COMPLETED))
                    .end()
                    .thenReply(Done.getInstance());
        } else if (windowStatusesNextToRun > 0){
            logger.info("[{}] Still running windows {}, run next windows: {}",
                    commandContext().workflowId(), windowStatusesRunningCount,windowStatusesNextToRun);
            return effects()
                    .updateState(updatedState)
                    .transitionTo(PositionBatchControllerWorkflow::launchWindowsStep)
                    .thenReply(Done.getInstance());
        } else {
            logger.info("[{}] Still running windows {}, no next windows to run. Pausing.",
                    commandContext().workflowId(), windowStatusesRunningCount);
            return effects()
                    .updateState(updatedState)
                    .pause()
                    .thenReply(Done.getInstance());
        }
    }

    @StepName("initialization")
    private StepEffect initializationStep() {
        logger.info("[{}] initializationStep", commandContext().workflowId());

        var state = currentState();
        // Count total positions to determine how many windows we need
        var totalPositions = taxDataRepository.countOpeningBalances(state.taxYear());

        if(totalPositions == 0){
            return stepEffects()
                    .updateState(state.withStatus(PositionBatchControllerState.ProcessingStatus.COMPLETED))
                    .thenEnd();
        }

        // Calculate number of windows based on batch size
        var positionsPerWindow = state.positionsPerWindow();
        var totalWindows = (int) Math.ceil((double) totalPositions / positionsPerWindow);

        state = state
                .withTotalWindows(totalWindows)
                .withTotalPositions(totalPositions)
                .prepareNextWindowBatchToLaunch();

        logger.info("[{}] initializationStep: totalPositions {}, positionsPerWindow {}, totalWindows {}, nextWindowBatchToLaunch {}", commandContext().workflowId(),  totalPositions, positionsPerWindow, totalWindows, state.getWindowStatusesToRun().size());
        return stepEffects()
            .updateState(state
                .withStatus(PositionBatchControllerState.ProcessingStatus.LAUNCHING_WINDOWS)
            )
            .thenTransitionTo(PositionBatchControllerWorkflow::launchWindowsStep);
    }

    @StepName("launch-windows")
    private StepEffect launchWindowsStep() {

        var state = currentState();
        logger.info("[{}] launchWindowsStep", commandContext().workflowId());
        var nextWindowBatches = state.getWindowStatusesToRun();
        var processingFutures = nextWindowBatches.stream().map(ws -> {
            logger.info("[{}] Launched sub-workflow {}",commandContext().workflowId(), ws.windowId());
            var startCommand = new PositionBatchWindowWorkflow.StartBatchCommand(
                    state.batchId(),
                    state.taxYear(),
                    ws.windowId(),
                    ws.windowOffset(),
                    ws.windowLimit(),
                    commandContext().workflowId()
            );
            return componentClient.forWorkflow(ws.windowWorkflowId())
                    .method(PositionBatchWindowWorkflow::start)
                    .invokeAsync(startCommand)
                    .toCompletableFuture();
        }).collect(Collectors.toList());

        CompletableFuture<?>[] futuresArray = processingFutures.toArray(new CompletableFuture<?>[0]);
        CompletableFuture<List<Done>> listFuture = CompletableFuture.allOf(futuresArray)
                .thenApply(v -> processingFutures.stream().map(CompletableFuture::join).collect(Collectors.toList()));
        listFuture.join();

        return stepEffects()
                .updateState(
                        state.withStatus(PositionBatchControllerState.ProcessingStatus.AWAITING_WINDOW_SUB_WORKFLOWS_CALLBACK)
                             .markWindowStatusesRunning(nextWindowBatches)
                )
                .thenPause();


    }


    @StepName("error-handling")
    private StepEffect errorHandlingStep() {
        logger.info("errorHandlingStep for {} window!", commandContext().workflowId());
        // Error occurred during processing
        return stepEffects()
            .updateState(currentState().withError("Batch processing failed due to system error"))
            .thenEnd();
    }
}