package com.example.application;

import akka.Done;
import akka.javasdk.annotations.Component;
import akka.javasdk.annotations.StepName;
import akka.javasdk.client.ComponentClient;
import akka.javasdk.workflow.Workflow;
import akka.stream.javadsl.Source;
import com.example.domain.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


/**
 * Main workflow orchestrating the complete opening balance batch processing.
 * Processes opening balance windows sequentially with callback-based sub-workflows.
 */
@Component(id = "batch-window")
public class BatchWindowASWorkflow extends Workflow<BatchWindowASState> {

    private static final Logger logger = LoggerFactory.getLogger(BatchWindowASWorkflow.class);

    private final ComponentClient componentClient;
    private final TaxDataRepository taxDataRepository;
    private final ProcessingConfig processingConfig;

    public BatchWindowASWorkflow(
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
//            .stepTimeout(BatchWindowASWorkflow::loadWindowStep, Duration.ofSeconds(60))
            .stepTimeout(BatchWindowASWorkflow::initializePositionsStep, Duration.ofSeconds(30))
            .stepTimeout(BatchWindowASWorkflow::launchTransactionProcessingStep, Duration.ofMinutes(5))
            .stepTimeout(BatchWindowASWorkflow::notifyParentStep, Duration.ofMinutes(1))
            .defaultStepRecovery(maxRetries(2).failoverTo(BatchWindowASWorkflow::errorHandlingStep))
            .build();
    }

    public record StartBatchCommand(String batchId, String taxYear, String windowId, int windowOffset, int windowLimit, String parentWorkflowId) {}

    public record BatchStatusResponse(
            String windowId,
            int windowOffset,
            int windowLimit,
            String batchId,
            String taxYear,
            Optional<String> errorMessage
    ) {}

    public record StreamCompletedCommand(
            int completedPositions,
            int totalPositions,
            Optional<String> errorMessage
    ) {}

    @Override
    public BatchWindowASState emptyState() {
        return BatchWindowASState.empty();
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
            .updateState(BatchWindowASState.initialize(command.windowId(),command.windowOffset(), command.windowLimit(), command.batchId(), command.taxYear(), command.parentWorkflowId()))
            .transitionTo(BatchWindowASWorkflow::initializePositionsStep)
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
            state.errorMessage()
        ));
    }

    /**
     * Callback method for sub-workflows to report completion.
     */
    public Effect<Done> onStreamCompleted(StreamCompletedCommand command) {
        final var state = currentState();


        if (state.status() == BatchWindowASState.ProcessingStatus.COMPLETED || state.status() == BatchWindowASState.ProcessingStatus.FAILED) {
            logger.info("[{}] Ignoring late completion notification", commandContext().workflowId());
            return effects().reply(Done.getInstance());
        }
        logger.info("[{}] Callback for stream completion: [{}]", commandContext().workflowId(), command);

        var updatedState = command.errorMessage.map(errMsg -> state.withError(command.completedPositions(),errMsg)).orElse(state.withCompleted(command.completedPositions()));

        return effects()
            .updateState(updatedState)
            .transitionTo(BatchWindowASWorkflow::notifyParentStep)
            .thenReply(Done.getInstance());
    }

//    @StepName("load-window")
//    private StepEffect loadWindowStep() {
//
//        var state = currentState();
//
//        // Load opening balances for current window
//        logger.info("[{}] Loading opening balances: taxYear={}, offset={}, limit={}", commandContext().workflowId(), state.taxYear(),  state.windowOffset(), state.windowLimit());
//        var openingBalances = taxDataRepository.loadOpeningBalancesBatch(
//            state.taxYear(), state.windowOffset() , state.windowLimit()
//        );
//
//        logger.info("[{}] Loaded {} opening balances ", commandContext().workflowId(), openingBalances.size());
//
//        // Store opening balances in state for next step to avoid reloading
//        var updatedState = state.withOpeningBalances(openingBalances);
//
//        return stepEffects()
//            .updateState(updatedState.withStatus(BatchWindowState.ProcessingStatus.INITIALIZING_POSITIONS))
//            .thenTransitionTo(BatchWindowASWorkflow::initializePositionsStep);
//    }

    @StepName("initialize-positions")
    private StepEffect initializePositionsStep() {
        var state = currentState();
        logger.info("[{}] initializePositionsStep", commandContext().workflowId());

        var openBalancesFlux = taxDataRepository.loadOpeningBalancesBatchFlux(state.taxYear(), state.windowOffset() , state.windowLimit());
        Source.fromPublisher(openBalancesFlux)
                .mapAsync(processingConfig.positionsPerBatch(), openingBalance ->
                        componentClient.forEventSourcedEntity(openingBalance.positionId().toEntityId())
                                .method(PositionEntity::initializeFromOpeningBalance)
                                .invokeAsync(openingBalance)
                        );


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
            .thenTransitionTo(BatchWindowASWorkflow::launchTransactionProcessingStep);
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
            .thenTransitionTo(BatchWindowASWorkflow::notifyParentStep);
    }
}