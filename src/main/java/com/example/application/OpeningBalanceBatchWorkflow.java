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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


/**
 * Main workflow orchestrating the complete opening balance batch processing.
 * Processes opening balance windows sequentially with callback-based sub-workflows.
 */
@Component(id = "opening-balance-batch")
public class OpeningBalanceBatchWorkflow extends Workflow<OpeningBalanceBatchState> {

    private static final Logger logger = LoggerFactory.getLogger(OpeningBalanceBatchWorkflow.class);

    private final ComponentClient componentClient;
    private final TaxDataRepository taxDataRepository;
    private final ProcessingConfig processingConfig;

    public OpeningBalanceBatchWorkflow(
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
            .stepTimeout(OpeningBalanceBatchWorkflow::initializationStep, Duration.ofSeconds(30))
            .stepTimeout(OpeningBalanceBatchWorkflow::loadWindowStep, Duration.ofSeconds(60))
            .stepTimeout(OpeningBalanceBatchWorkflow::initializePositionsStep, Duration.ofSeconds(30))
            .stepTimeout(OpeningBalanceBatchWorkflow::launchTransactionProcessingStep, Duration.ofMinutes(5))
            .defaultStepRecovery(maxRetries(2).failoverTo(OpeningBalanceBatchWorkflow::errorHandlingStep))
            .build();
    }

    public record StartBatchCommand(String batchId, String taxYear) {}

    public record BatchStatusResponse(
        String batchId,
        String taxYear,
        OpeningBalanceBatchState.ProcessingStatus status,
        long totalPositions,
        int totalWindows,
        int currentWindow,
        Map<Integer, OpeningBalanceBatchState.Window> windows,
        String errorMessage
    ) {}

    public record SubWorkflowCompletedCommand(
        String subWorkflowId,
        int positionsCurrentWindow,
        int completedPositions,
        int totalPositions,
        OpeningBalanceTransactionsBatchState.ProcessingStatus status,
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
    public OpeningBalanceBatchState emptyState() {
        return OpeningBalanceBatchState.empty();
    }

    /**
     * Start the complete opening balance batch processing.
     */
    public Effect<Done> start(StartBatchCommand command) {
        logger.info("Starting OpeningBalanceBatchWorkflow: {}", commandContext().workflowId());

        return effects()
            .updateState(OpeningBalanceBatchState.initialize(command.batchId(), command.taxYear()))
            .transitionTo(OpeningBalanceBatchWorkflow::initializationStep)
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
            state.currentWindow(),
            state.windows(),
            state.errorMessage()
        ));
    }

    /**
     * Callback method for sub-workflows to report completion.
     */
    public Effect<Done> onSubWorkflowCompleted(SubWorkflowCompletedCommand command) {
        var state = currentState();

        if(state.currentWindow()!=command.positionsCurrentWindow()){
            logger.info("[{}] Ignoring late sub-workflow completion notification for {} from already completed workflow from old window {} (current window {})",
                    commandContext().workflowId(), command.subWorkflowId(), command.positionsCurrentWindow(), state.currentWindow());
            return effects().reply(Done.getInstance());

        }
        if (!state.windows().containsKey(state.currentWindow())) {
            logger.info("[{}] Ignoring late sub-workflow completion notification for {} from non existing window {} (current window {})",
                    commandContext().workflowId(), command.subWorkflowId(),  command.positionsCurrentWindow(), state.currentWindow());
            return effects().reply(Done.getInstance());
        }

        if (state.status() == OpeningBalanceBatchState.ProcessingStatus.COMPLETED || state.windows().get(state.currentWindow()).subWorkflowStates().containsKey(command.subWorkflowId())) {
            logger.info("[{}] Ignoring late sub-workflow completion notification for {} from already completed workflow",
                commandContext().workflowId(), command.subWorkflowId());
            return effects().reply(Done.getInstance());
        }
        logger.info("[{}] Callback for sub-workflow {} received", commandContext().workflowId(), command.subWorkflowId());

        // Add completed sub-workflow to state
        var updatedState = state.addSubWorkflowState(state.currentWindow(),
            new OpeningBalanceBatchState.SubWorkflowStatus(
                command.subWorkflowId(),
                command.status(),
                command.completedPositions(),
                command.totalPositions(),
                command.errorMessage()
            ));


        if(updatedState.windows().get(state.currentWindow()).subWorkflowStates().size() == updatedState.windows().get(state.currentWindow()).totalSubWorkflows()){
            var newWindow = updatedState.currentWindow() + 1;
            logger.info("[{}] All sub-workflows completed successfully for window {}. Loading new window {}",
                    commandContext().workflowId(), updatedState.currentWindow(), newWindow);
            return effects()
                    .updateState(updatedState
                            .withStatus(OpeningBalanceBatchState.ProcessingStatus.COMPLETED)
                            .withCurrentWindow(newWindow)
                    )
                    .transitionTo(OpeningBalanceBatchWorkflow::loadWindowStep)
                    .thenReply(Done.getInstance());

        }

        return effects()
            .updateState(updatedState)
            .pause()
            .thenReply(Done.getInstance());
    }

    @StepName("initialization")
    private StepEffect initializationStep() {
        logger.info("[{}] initializationStep", commandContext().workflowId());
        // Count total positions to determine how many windows we need
        var totalPositions = taxDataRepository.countOpeningBalances(currentState().taxYear());

        // Calculate number of windows based on batch size
        var positionsPerWindow = processingConfig.openingBalanceBatchSize();
        var totalWindows = (int) Math.ceil((double) totalPositions / positionsPerWindow);

        return stepEffects()
            .updateState(currentState()
                .withTotalPositions(totalPositions)
                .withTotalWindow(totalWindows)
                .withCurrentWindow(0)
                .withStatus(OpeningBalanceBatchState.ProcessingStatus.LOAD_NEXT_WINDOW)
            )
            .thenTransitionTo(OpeningBalanceBatchWorkflow::loadWindowStep);
    }

    @StepName("load-window")
    private StepEffect loadWindowStep() {

        var state = currentState();

        var currentWindow = state.currentWindow();
        logger.info("[{}] loadWindowStep: {}", commandContext().workflowId(), currentWindow);
        if (currentWindow >= state.totalWindows()) {
            logger.info("[{}] All windows completed ({} >= {})", commandContext().workflowId(), currentWindow, state.totalWindows());
            // All windows completed
            return stepEffects()
                .updateState(state.withStatus(OpeningBalanceBatchState.ProcessingStatus.COMPLETED))
                .thenEnd();
        }

        var offset = currentWindow * processingConfig.openingBalanceBatchSize();
        //TODO limit should go to state so it can't change via config when workflow started
        var limit = processingConfig.openingBalanceBatchSize();


        // Load opening balances for current window
        logger.info("[{}] Loading opening balances: taxYear={}, offset={}, limit={}", commandContext().workflowId(), state.taxYear(), offset, limit);
        var openingBalances = taxDataRepository.loadOpeningBalancesBatch(
            state.taxYear(), offset, limit
        );

        logger.info("[{}] Loaded {} opening balances for window {}", commandContext().workflowId(), openingBalances.size(), currentWindow);

        // Store opening balances in state for next step to avoid reloading
        var updatedState = state.withOpeningBalancesForCurrentWindow(state.currentWindow(), openingBalances);

        return stepEffects()
            .updateState(updatedState.withStatus(OpeningBalanceBatchState.ProcessingStatus.INITIALIZING_POSITIONS))
            .thenTransitionTo(OpeningBalanceBatchWorkflow::initializePositionsStep);
    }

    @StepName("initialize-positions")
    private StepEffect initializePositionsStep() {
        var state = currentState();
        var currentWindow = state.currentWindow();
        logger.info("[{}] initializePositionsStep window {}", commandContext().workflowId(), currentWindow);

        // Use opening balances already loaded in previous step
        var openingBalances = state.windows().get(state.currentWindow()).openingBalances();

        logger.info("[{}] Using {} cached opening balances for window {}",  commandContext().workflowId(), openingBalances.size(), currentWindow);

        // Process each transaction in this window using async calls
        var processingFutures = openingBalances.stream()
                .map(openingBalance ->
                    componentClient.forEventSourcedEntity(openingBalance.positionId().toEntityId())
                            .method(PositionEntity::initializeFromOpeningBalance)
                            .invokeAsync(openingBalance)
                )
                .toList();

        // Wait for all transactions in this window to complete
        var allProcessed = CompletableFuture.allOf(
                processingFutures.toArray(new CompletableFuture[0])
        );

        // Materialize the futures - wait for completion
        allProcessed.join();

        logger.info("[{}] initializePositionsStep window {} DONE!", commandContext().workflowId(), currentWindow);
        return stepEffects()
            .updateState(state.withStatus(OpeningBalanceBatchState.ProcessingStatus.LAUNCHING_TRANSACTION_PROCESSING))
            .thenTransitionTo(OpeningBalanceBatchWorkflow::launchTransactionProcessingStep);
    }

    @StepName("launch-transaction-processing")
    private StepEffect launchTransactionProcessingStep() {
        var state = currentState();
        var currentWindow = state.currentWindow();
        logger.info("[{}] launchTransactionProcessingStep window {}", commandContext().workflowId(), currentWindow);

        // Use opening balances already loaded in previous step
        var openingBalances = state.windows().get(state.currentWindow()).openingBalances();

        var positionIds = openingBalances.stream()
            .map(OpeningBalance::positionId)
            .toList();

        logger.info(" [{}] Using {} opening balances, {} position IDs for sub-workflow processing",
                commandContext().workflowId(), openingBalances.size(), positionIds.size());

        // Calculate number of positionBatches based on batch size
        var positionsPerBatch = processingConfig.transactionMicrobatchSize();
        var totalPositionBatches = (int) Math.ceil((double) positionIds.size() / positionsPerBatch);
        logger.info("[{}] Using positionsPerBatch {} / totalPositionBatches {}", commandContext().workflowId(), positionsPerBatch, totalPositionBatches);

        var processingFutures = IntStream.range(0, totalPositionBatches).mapToObj(positionBatchIndex -> {
            var fromIndex = positionBatchIndex * positionsPerBatch;
            var toIndex = fromIndex + positionsPerBatch;
            if(toIndex >= openingBalances.size()) {
                toIndex = openingBalances.size()-1;
            }
            var subList = positionIds.subList(fromIndex, toIndex);
            var startCommand = new OpeningBalanceTransactionsBatchWorkflow.StartCommand(
                    state.batchId(),
                    state.taxYear(),
                    subList,
                    state.currentWindow(),
                    commandContext().workflowId()
            );
            var subWorkflowId = new OpeningBalanceTransactionsBatchWorkflowId(state.batchId(), currentWindow, positionBatchIndex);
            logger.info("[{}] Launched sub-workflow {}",commandContext().workflowId(), subWorkflowId);
            return componentClient.forWorkflow(subWorkflowId.serialize())
                    .method(OpeningBalanceTransactionsBatchWorkflow::start)
                    .invokeAsync(startCommand)
                    .thenApply(result -> subWorkflowId)
                    .toCompletableFuture();
        }).toList();

        CompletableFuture<?>[] futuresArray = processingFutures.toArray(new CompletableFuture<?>[0]);
        CompletableFuture<List<OpeningBalanceTransactionsBatchWorkflowId>> listFuture = CompletableFuture.allOf(futuresArray)
                .thenApply(v -> processingFutures.stream().map(CompletableFuture::join).collect(Collectors.toList()));
        final List<OpeningBalanceTransactionsBatchWorkflowId> subWorkflowIds = listFuture.join();

        logger.info("[{}] launchTransactionProcessingStep  window {} DONE!", commandContext().workflowId(), currentWindow);
        // Transition to awaiting callbacks
        return stepEffects()
            .updateState(
                    state.withTotalSubWorkflows(state.currentWindow(), subWorkflowIds.size())
                         .withStatus(OpeningBalanceBatchState.ProcessingStatus.AWAITING_TRANSACTION_SUB_WORKFLOWS_CALLBACK))
            .thenPause(); // Workflow pauses until callbacks resume it
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