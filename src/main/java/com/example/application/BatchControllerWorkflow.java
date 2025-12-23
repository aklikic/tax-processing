package com.example.application;

import akka.Done;
import akka.javasdk.annotations.Component;
import akka.javasdk.annotations.StepName;
import akka.javasdk.client.ComponentClient;
import akka.javasdk.workflow.Workflow;
import com.example.domain.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


/**
 * Main workflow orchestrating the complete opening balance batch processing.
 * Processes opening balance windows sequentially with callback-based sub-workflows.
 */
@Component(id = "batch-controller")
public class BatchControllerWorkflow extends Workflow<BatchControllerState> {

    private static final Logger logger = LoggerFactory.getLogger(BatchControllerWorkflow.class);

    private final ComponentClient componentClient;
    private final TaxDataRepository taxDataRepository;
    private final ProcessingConfig processingConfig;

    public BatchControllerWorkflow(
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
            .stepTimeout(BatchControllerWorkflow::initializationStep, Duration.ofSeconds(30))
            .stepTimeout(BatchControllerWorkflow::launchWindowsStep, Duration.ofSeconds(60))
            .defaultStepRecovery(maxRetries(2).failoverTo(BatchControllerWorkflow::errorHandlingStep))
            .build();
    }

    public record StartBatchCommand(String batchId, String taxYear) {}

    public record BatchStatusResponse(
        String batchId,
        String taxYear,
        BatchControllerState.ProcessingStatus status,
        long totalPositions,
        int totalWindows,
        Map<String, BatchControllerState.WindowStatus> windowStatuses,
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
    public BatchControllerState emptyState() {
        return BatchControllerState.empty();
    }

    /**
     * Start the complete opening balance batch processing.
     */
    public Effect<Done> start(StartBatchCommand command) {
        logger.info("Starting OpeningBalanceBatchWorkflow: {}", commandContext().workflowId());

        return effects()
            .updateState(BatchControllerState.initialize(command.batchId(), command.taxYear(), processingConfig.maxParallelWindows(), processingConfig.positionsPerWindow()))
            .transitionTo(BatchControllerWorkflow::initializationStep)
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
            state.windowStatuses(),
            state.completedPositions(),
            state.errorMessage()
        ));
    }

    /**
     * Callback method for sub-workflows to report completion.
     */
    public Effect<Done> onSubWorkflowCompleted(SubWorkflowCompletedCommand command) {
        var state = currentState();

        if (state.status() == BatchControllerState.ProcessingStatus.COMPLETED || state.windowStatuses().get(command.windowId()).status() != BatchControllerState.WindowProcessingStatus.RUNNING) {
            logger.info("[{}] Ignoring late sub-workflow completion notification for {}",
                    commandContext().workflowId(), command.windowId());
            return effects().reply(Done.getInstance());
        }

        logger.info("[{}] Callback for sub-workflow {} received", commandContext().workflowId(), command.windowId());

        // Add completed sub-workflow to state
        var updatedState = state.onWindowStatusResult(command.windowId(), command.windowCompletedPositions(), Optional.ofNullable(command.errorMessage));

        // prepare next window batch to launch
        updatedState = updatedState.prepareNextWindowBatchToLaunch();

        if(updatedState.getWindowStatusesToRun().isEmpty()){
            logger.info("[{}] All sub-workflows completed successfully. Not more windows to run. DONE!",
                    commandContext().workflowId());
            return effects()
                    .updateState(updatedState.withStatus(BatchControllerState.ProcessingStatus.COMPLETED))
                    .end()
                    .thenReply(Done.getInstance());
        }else {
            logger.info("[{}] Run next windows: {}",
                    commandContext().workflowId(), updatedState.getWindowStatusesToRun().size());
            return effects()
                    .updateState(updatedState)
                    .transitionTo(BatchControllerWorkflow::launchWindowsStep)
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
                    .updateState(state.withStatus(BatchControllerState.ProcessingStatus.COMPLETED))
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
                .withStatus(BatchControllerState.ProcessingStatus.LAUNCHING_WINDOWS)
            )
            .thenTransitionTo(BatchControllerWorkflow::launchWindowsStep);
    }

    @StepName("launch-windows")
    private StepEffect launchWindowsStep() {

        var state = currentState();
        logger.info("[{}] launchWindowsStep", commandContext().workflowId());
        var nextWindowBatches = state.getWindowStatusesToRun();
        var processingFutures = nextWindowBatches.stream().map(ws -> {
            logger.info("[{}] Launched sub-workflow {}",commandContext().workflowId(), ws.windowId());
            var startCommand = new BatchWindowWorkflow.StartBatchCommand(
                    state.batchId(),
                    state.taxYear(),
                    ws.windowId(),
                    ws.windowOffset(),
                    ws.windowLimit(),
                    commandContext().workflowId()
            );
            return componentClient.forWorkflow(ws.windowWorkflowId())
                    .method(BatchWindowWorkflow::start)
                    .invokeAsync(startCommand)
                    .toCompletableFuture();
        }).collect(Collectors.toList());

        CompletableFuture<?>[] futuresArray = processingFutures.toArray(new CompletableFuture<?>[0]);
        CompletableFuture<List<Done>> listFuture = CompletableFuture.allOf(futuresArray)
                .thenApply(v -> processingFutures.stream().map(CompletableFuture::join).collect(Collectors.toList()));
        listFuture.join();

        return stepEffects()
                .updateState(state.withStatus(BatchControllerState.ProcessingStatus.AWAITING_WINDOW_SUB_WORKFLOWS_CALLBACK))
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