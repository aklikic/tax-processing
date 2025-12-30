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
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


/**
 * Main workflow orchestrating the complete opening balance batch processing.
 * Processes opening balance windows sequentially with callback-based sub-workflows.
 */
@Component(id = "batch-window")
public class BatchWindowWorkflow extends Workflow<BatchWindowState> {

    private static final Logger logger = LoggerFactory.getLogger(BatchWindowWorkflow.class);

    private final ComponentClient componentClient;
    private final TaxDataRepository taxDataRepository;
    private final ProcessingConfig processingConfig;

    public BatchWindowWorkflow(
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
            .stepTimeout(BatchWindowWorkflow::loadWindowStep, Duration.ofSeconds(60))
            .stepTimeout(BatchWindowWorkflow::initializePositionsStep, Duration.ofSeconds(30))
            .stepTimeout(BatchWindowWorkflow::launchTransactionProcessingStep, Duration.ofMinutes(5))
            .stepTimeout(BatchWindowWorkflow::notifyParentStep, Duration.ofMinutes(1))
            .defaultStepRecovery(maxRetries(2).failoverTo(BatchWindowWorkflow::errorHandlingStep))
            .build();
    }

    public record StartBatchCommand(String batchId, String taxYear, String windowId, int windowOffset, int windowLimit, String parentWorkflowId) {}

    public record BatchStatusResponse(
            String windowId,
            int windowOffset,
            int windowLimit,
            String batchId,
            String taxYear,
            BatchWindowState.ProcessingStatus status,
            int totalSubWorkflows,
            Map<String, BatchWindowState.SubWorkflowStatus> subWorkflowStates,
            String errorMessage
    ) {}

    public record SubWorkflowCompletedCommand(
        String subWorkflowId,
        int completedPositions,
        int totalPositions,
        OpeningBalanceTransactionsBatchState.ProcessingStatus status,
        String errorMessage
    ) {}

    private record OpeningBalanceTransactionsBatchWorkflowId(String batchId, String windowId, int positionBatchIndex){
        public String serialize() {
            return batchId + "-" + windowId + "-" + positionBatchIndex;
        }
        public static OpeningBalanceTransactionsBatchWorkflowId deserialize(String raw) {
            var split = raw.split("-");
            return new OpeningBalanceTransactionsBatchWorkflowId(split[0], split[1], Integer.parseInt(split[2]));
        }
    }

    @Override
    public BatchWindowState emptyState() {
        return BatchWindowState.empty();
    }

    /**
     * Start the complete opening balance batch processing.
     */
    public Effect<Done> start(StartBatchCommand command) {
        if(!currentState().isEmpty()){
            logger.error("Starting already started workflow: {}", commandContext().workflowId());
            return effects().reply(Done.getInstance());
        }
        logger.info("Starting: {}", commandContext().workflowId());
        return effects()
            .updateState(BatchWindowState.initialize(command.windowId(),command.windowOffset(), command.windowLimit(), command.batchId(), command.taxYear(), command.parentWorkflowId()))
            .transitionTo(BatchWindowWorkflow::loadWindowStep)
            .thenReply(Done.getInstance());
    }

    /**
     * Get current batch processing status.
     */
    public Effect<BatchStatusResponse> getStatus() {
        var state = currentState();
        return effects().reply(new BatchStatusResponse(
            state.windowId(),
            state.windowOffset(),
            state.windowLimit(),
            state.batchId(),
            state.taxYear(),
            state.status(),
            state.totalSubWorkflows(),
            state.subWorkflowStates(),
            state.errorMessage()
        ));
    }

    /**
     * Callback method for sub-workflows to report completion.
     */
    public Effect<Done> onSubWorkflowCompleted(SubWorkflowCompletedCommand command) {
        var state = currentState();


        if (state.status() == BatchWindowState.ProcessingStatus.COMPLETED || state.subWorkflowStates().containsKey(command.subWorkflowId())) {
            logger.info("[{}] Ignoring late sub-workflow completion notification for {} from already completed workflow",
                commandContext().workflowId(), command.subWorkflowId());
            return effects().reply(Done.getInstance());
        }
        logger.info("[{}] Callback for sub-workflow {} received", commandContext().workflowId(), command.subWorkflowId());

        // Add completed sub-workflow to state
        var updatedState = state.addSubWorkflow(
            new BatchWindowState.SubWorkflowStatus(
                command.subWorkflowId(),
                command.status(),
                command.completedPositions(),
                command.totalPositions(),
                command.errorMessage()
            ));
        if(updatedState.subWorkflowStates().size() == updatedState.totalSubWorkflows()){
            logger.info("[{}] All sub-workflows completed successfully",
                    commandContext().workflowId());
            return effects()
                    .updateState(updatedState
                            .withStatus(BatchWindowState.ProcessingStatus.COMPLETED)
                    )
                    .transitionTo(BatchWindowWorkflow::notifyParentStep)
                    .thenReply(Done.getInstance());
        }

        return effects()
            .updateState(updatedState)
            .pause()
            .thenReply(Done.getInstance());
    }

    @StepName("load-window")
    private StepEffect loadWindowStep() {

        var state = currentState();

        // Load opening balances for current window
        logger.info("[{}] Loading opening balances: taxYear={}, offset={}, limit={}", commandContext().workflowId(), state.taxYear(),  state.windowOffset(), state.windowLimit());
        var openingBalances = taxDataRepository.loadOpeningBalancesBatch(
            state.taxYear(), state.windowOffset() , state.windowLimit()
        );

        logger.info("[{}] Loaded {} opening balances ", commandContext().workflowId(), openingBalances.size());

        // Store opening balances in state for next step to avoid reloading
        var updatedState = state.withOpeningBalances(openingBalances);

        return stepEffects()
            .updateState(updatedState.withStatus(BatchWindowState.ProcessingStatus.INITIALIZING_POSITIONS))
            .thenTransitionTo(BatchWindowWorkflow::initializePositionsStep);
    }

    @StepName("initialize-positions")
    private StepEffect initializePositionsStep() {
        var state = currentState();
        logger.info("[{}] initializePositionsStep", commandContext().workflowId());

        // Use opening balances already loaded in previous step
        var openingBalances = state.openingBalances();

        logger.info("[{}] Using {} cached opening balances",  commandContext().workflowId(), openingBalances.size());

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

        logger.info("[{}] initializePositionsStep DONE!", commandContext().workflowId());
        return stepEffects()
            .updateState(state.withStatus(BatchWindowState.ProcessingStatus.LAUNCHING_TRANSACTION_PROCESSING))
            .thenTransitionTo(BatchWindowWorkflow::launchTransactionProcessingStep);
    }

    @StepName("launch-transaction-processing")
    private StepEffect launchTransactionProcessingStep() {
        var state = currentState();
        logger.info("[{}] launchTransactionProcessingStep", commandContext().workflowId());

        // Use opening balances already loaded in previous step
        var openingBalances = state.openingBalances();

        var positionIds = openingBalances.stream()
            .map(OpeningBalance::positionId)
            .toList();

        logger.info(" [{}] Using {} opening balances, {} position IDs for sub-workflow processing",
                commandContext().workflowId(), openingBalances.size(), positionIds.size());

        // Calculate number of positionBatches based on batch size
        var positionsPerBatch = processingConfig.positionsPerBatch();
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
                    commandContext().workflowId()
            );
            var subWorkflowId = new OpeningBalanceTransactionsBatchWorkflowId(state.batchId(), state.windowId(), positionBatchIndex);
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

        logger.info("[{}] launchTransactionProcessingStep DONE!", commandContext().workflowId());
        // Transition to awaiting callbacks
        return stepEffects()
            .updateState(
                    state.withTotalSubWorkflows(subWorkflowIds.size())
                         .withStatus(BatchWindowState.ProcessingStatus.AWAITING_TRANSACTION_SUB_WORKFLOWS_CALLBACK))
            .thenPause(); // Workflow pauses until callbacks resume it
    }


    @StepName("notify-parent")
    private StepEffect notifyParentStep() {
        var state = currentState();
        var isSuccess = state.status() == BatchWindowState.ProcessingStatus.COMPLETED;

        if (isSuccess) {
            logger.info("[{}] Successfully completed batch processing",
                    commandContext().workflowId());
        } else {
            logger.error("[{}] Processing failed: {}",
                    commandContext().workflowId(), state.errorMessage());
        }

        var callbackCommand = new BatchControllerWorkflow.SubWorkflowCompletedCommand(
               state.windowId(),
               state.getCompletedPositionsCount(),
               state.errorMessage()
        );

        logger.info("[{}] Notifying parent workflow {} of completion.",
                commandContext().workflowId(),
                state.parentWorkflowId());

        componentClient.forWorkflow(state.parentWorkflowId())
                .method(BatchControllerWorkflow::onSubWorkflowCompleted)
                .invoke(callbackCommand);

        return stepEffects().thenEnd();
    }
    @StepName("error-handling")
    private StepEffect errorHandlingStep() {
        logger.info("errorHandlingStep for {} window!", commandContext().workflowId());
        // Error occurred during processing
        return stepEffects()
            .updateState(currentState().withError("Batch processing failed due to system error"))
            .thenTransitionTo(BatchWindowWorkflow::notifyParentStep);
    }
}