package com.example.application;

import akka.Done;
import akka.javasdk.annotations.Component;
import akka.javasdk.annotations.StepName;
import akka.javasdk.client.ComponentClient;
import akka.javasdk.workflow.Workflow;
import akka.stream.Materializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.example.domain.PositionBatchWindowState;
import com.example.domain.ProcessingConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;


/**
 * Main workflow orchestrating the complete opening balance batch processing.
 * Processes opening balance windows sequentially with callback-based sub-workflows.
 */
@Component(id = "position-batch-window")
public class PositionBatchWindowWorkflow extends Workflow<PositionBatchWindowState> {

    private static final Logger logger = LoggerFactory.getLogger(PositionBatchWindowWorkflow.class);

    private final ComponentClient componentClient;
    private final TaxDataRepository taxDataRepository;
    private final ProcessingConfig processingConfig;
    private final Materializer materializer;

    public PositionBatchWindowWorkflow(
        ComponentClient componentClient,
        TaxDataRepository taxDataRepository,
        ProcessingConfig processingConfig,
        Materializer materializer
    ) {
        this.componentClient = componentClient;
        this.taxDataRepository = taxDataRepository;
        this.processingConfig = processingConfig;
        this.materializer = materializer;
    }

    @Override
    public WorkflowSettings settings() {
        return WorkflowSettings.builder()

            .stepTimeout(PositionBatchWindowWorkflow::notifyParentStep, Duration.ofMinutes(1))
            .defaultStepRecovery(maxRetries(0).failoverTo(PositionBatchWindowWorkflow::errorHandlingStep))
            .build();
    }

    public record StartBatchCommand(String batchId, String taxYear, String positionWindowId, int positionWindowOffset, int positionWindowLimit, String parentWorkflowId) {}

    public record BatchStatusResponse(
            String windowId,
            int windowOffset,
            int windowLimit,
            String batchId,
            String taxYear,
            PositionBatchWindowState.ProcessingStatus status,
            Optional<String> errorMessage
    ) {}

    public record CompleteCommand(
            Optional<String> errorMessage
    ) {}

    public record CommitOffsetCommand(
            int offset
    ) {}

    @Override
    public PositionBatchWindowState emptyState() {
        return PositionBatchWindowState.empty();
    }

    /**
     * Get current batch processing status.
     */
    public Effect<BatchStatusResponse> getStatus() {
        var state = currentState();
        return effects().reply(new BatchStatusResponse(
            state.positionWindowId(),
            state.positionWindowOffset(),
            state.positionWindowLimit(),
            state.batchId(),
            state.taxYear(),
            state.status(),
            state.errorMessage()
        ));
    }
    public Effect<Done> offsetCommit(CommitOffsetCommand command) {
        final var state = currentState();
        if (state.status() != PositionBatchWindowState.ProcessingStatus.RUNNING) {
            logger.error("[{}] Ignoring late offsetCommit notification", commandContext().workflowId());
            return effects().reply(Done.getInstance());
        }
        logger.debug("[{}] Callback for offsetCommit: [{}]", commandContext().workflowId(), command);

        return effects()
                .updateState(state.withTransactionBatchOffset(command.offset()))
                .pause(
                     pauseSetting(processingConfig.transactionsBatchProcessingTimeout())
                                                  .timeoutHandler(PositionBatchWindowWorkflow::runningTimeout)
                ).thenReply(Done.getInstance());
    }

    public Effect<Done> complete(CompleteCommand command) {
        final var state = currentState();

        if (state.status() != PositionBatchWindowState.ProcessingStatus.RUNNING) {
            logger.error("[{}] Ignoring late complete notification", commandContext().workflowId());
            return effects().reply(Done.getInstance());
        }
        if(command.errorMessage().isPresent()){
            logger.error("[{}] Callback error: {}", commandContext().workflowId(), command.errorMessage().get());
        } else {
            logger.info("[{}] Callback for complete!", commandContext().workflowId());
        }


        if(command.errorMessage().isPresent()){
            var updatedState = state.withError(command.errorMessage().get()).withRetriesIncrease();
            var maxRetries = 2; //TODO add to config
            if(updatedState.retries() >= maxRetries){
                return effects()
                        .updateState(updatedState)
                        .transitionTo(PositionBatchWindowWorkflow::notifyParentStep)
                        .thenReply(Done.getInstance());
            }else{
                updatedState = updatedState.withStatus(PositionBatchWindowState.ProcessingStatus.START);
                logger.info("[{}] Retry startStep: [{}]", commandContext().workflowId(), updatedState.retries());
                return effects()
                        .updateState(updatedState)
                        .transitionTo(PositionBatchWindowWorkflow::startStep)
                        .thenReply(Done.getInstance());
            }

        }else{
            return effects()
                    .updateState(state.withStatus(PositionBatchWindowState.ProcessingStatus.COMPLETED))
                    .transitionTo(PositionBatchWindowWorkflow::notifyParentStep)
                    .thenReply(Done.getInstance());
        }
    }

    public Effect<Done> runningTimeout() {
        final var state = currentState();
        if (state.status() != PositionBatchWindowState.ProcessingStatus.RUNNING) {
            logger.error("[{}] Ignoring late acceptanceTimeout notification", commandContext().workflowId());
            return effects().reply(Done.getInstance());
        }
        logger.debug("[{}] Callback for acceptanceTimeout. Retry startStep.", commandContext().workflowId());

        return effects()
                .updateState(state.withStatus(PositionBatchWindowState.ProcessingStatus.START))
                .transitionTo(PositionBatchWindowWorkflow::startStep)
                .thenReply(Done.getInstance());
    }

    /**
     * Start the complete opening balance batch processing.
     */
    public Effect<Done> start(StartBatchCommand command) {
        if(!currentState().isEmpty()){
            logger.error("Starting already started workflow: {}", commandContext().workflowId());
            return effects().reply(Done.getInstance());
        }
        logger.debug("Initializing: {}", commandContext().workflowId());
        return effects()
                .updateState(PositionBatchWindowState.init(command.positionWindowId(),command.positionWindowOffset(), command.positionWindowLimit(), command.batchId(), command.taxYear(), command.parentWorkflowId()))
                .transitionTo(PositionBatchWindowWorkflow::initStep)
                .thenReply(Done.getInstance());
    }

    @StepName("init")
    private StepEffect initStep() {
        final var state = currentState();
        logger.debug("initStep: {}", commandContext().workflowId());
        var transactionCount = taxDataRepository.countTransactionsForPositionWindow(state.taxYear(), state.positionWindowOffset(), state.positionWindowLimit());

        var transactionBatchCount = (int) Math.ceil((double) transactionCount / processingConfig.transactionsBatchLimit());

        logger.info("[{}] Starting: transactionBatchCount={}, transactionCount={}, transactionsPerBatch={}", commandContext().workflowId(),transactionBatchCount, transactionCount, processingConfig.transactionsBatchLimit());
        return stepEffects()
                .updateState(state.start(transactionCount,transactionBatchCount,processingConfig.transactionsBatchLimit()))//TODO rename transactionsBatchLimit
                .thenTransitionTo(PositionBatchWindowWorkflow::startStep);
    }

    @StepName("start")
    private StepEffect startStep() {
        final var state = currentState();

        var startTransactionBatchOffset = state.transactionBatchOffset() + 1;
        logger.debug("[{}] startStep: positionWindowOffset={}, positionWindowLimit={}, transactionBatchCount={}, transactionsPerBatch={}, startTransactionBatchOffset={}",
                commandContext().workflowId(), state.positionWindowOffset(), state.positionWindowLimit(), state.transactionBatchCount(), state.transactionsPerBatch(), startTransactionBatchOffset);

        if(startTransactionBatchOffset >= state.transactionBatchCount()){
            logger.info("[{}] startStep all transaction batches are done! transactionBatchCount={}, startTransactionBatchOffset={}",
                    commandContext().workflowId(), state.transactionBatchCount(), startTransactionBatchOffset);
            return stepEffects()
                    .updateState(state.withStatus(PositionBatchWindowState.ProcessingStatus.COMPLETED))
                    .thenTransitionTo(PositionBatchWindowWorkflow::notifyParentStep);
        }

        final var myWorkflowId = commandContext().workflowId();

        Flow<Integer, Done, ?> transactionBatchFlow =
                Flow.<Integer>create()
                    .mapAsync( 1, transactionBatchIndex -> {
                        var offset = transactionBatchIndex * state.transactionsPerBatch();
                        logger.debug("[{}] transactionBatchFlow: offset={}, transactionsPerBatch={}", myWorkflowId, offset, state.transactionsPerBatch());
                        return Source.fromPublisher(taxDataRepository.loadTransactionsForPositionWindow(state.taxYear(), state.positionWindowOffset(), state.positionWindowLimit(), offset, state.transactionsPerBatch()))
                                     .mapAsyncPartitioned(processingConfig.transactionsBatchParallelism(), 1, t -> t.positionId().toEntityId(),  (transaction, positionEntityId) -> {
                                         return componentClient.forEventSourcedEntity(positionEntityId)
                                                 .method(PositionEntity::processTransaction)
                                                 .invokeAsync(transaction)
                                                 .thenApply(tr -> Done.getInstance());
                                     }).toMat(Sink.ignore(),Keep.right())
                                .run(materializer)
                                .thenApply(d -> transactionBatchIndex)
                                .exceptionally(e -> {
                                    logger.error("[{}] transactionBatchFlow: offset={}, transactionsPerBatch={}: {}", myWorkflowId, offset, state.transactionsPerBatch(), e);
                                    throw new RuntimeException(e);
                                });
                    }).mapAsync(1, transactionBatchIndex ->
                        componentClient.forWorkflow(myWorkflowId).method(PositionBatchWindowWorkflow::offsetCommit).invokeAsync(new PositionBatchWindowWorkflow.CommitOffsetCommand(transactionBatchIndex))
                    );

        //TODO add hook for stop

        Source.range(startTransactionBatchOffset, state.transactionBatchCount())
                .via(transactionBatchFlow)
                .toMat(Sink.ignore(),Keep.right())
                .run(materializer)
                .handleAsync((d, e) -> {
                    var cmd = new CompleteCommand(Optional.ofNullable(e).map(Throwable::getMessage));
                    return componentClient.forWorkflow(myWorkflowId).method(PositionBatchWindowWorkflow::complete).invokeAsync(cmd);
                });

        logger.debug("[{}] startStep streaming running!", commandContext().workflowId());
        return stepEffects()
                .updateState(state.withStatus(PositionBatchWindowState.ProcessingStatus.RUNNING))
                .thenPause(
                     pauseSetting(processingConfig.transactionsBatchProcessingTimeout())
                                                  .timeoutHandler(PositionBatchWindowWorkflow::runningTimeout)
                );
    }



    @StepName("notify-parent")
    private StepEffect notifyParentStep() {
        var state = currentState();
        var isSuccess = state.status() == PositionBatchWindowState.ProcessingStatus.COMPLETED;

        if (isSuccess) {
            logger.info("[{}] Successfully completed batch processing",  commandContext().workflowId());
        } else {
            logger.error("[{}] Processing failed: {}", commandContext().workflowId(), state.errorMessage());
        }

        var callbackCommand = new PositionBatchControllerWorkflow.SubWorkflowCompletedCommand(
               state.positionWindowId(),
               state.positionWindowLimit(),
               state.errorMessage().orElse(null)
        );

        logger.debug("[{}] Notifying parent workflow {} of completion.",
                commandContext().workflowId(),
                state.parentWorkflowId());

        componentClient.forWorkflow(state.parentWorkflowId())
                .method(PositionBatchControllerWorkflow::onSubWorkflowCompleted)
                .invoke(callbackCommand);

        return stepEffects().thenEnd();
    }
    @StepName("error-handling")
    private StepEffect errorHandlingStep() {
        logger.error("errorHandlingStep for {} window!", commandContext().workflowId());
        // Error occurred during processing
        return stepEffects()
            .updateState(currentState().withError("Batch processing failed due to system error"))
            .thenTransitionTo(PositionBatchWindowWorkflow::notifyParentStep);
    }
}