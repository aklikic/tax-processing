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
import com.example.domain.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;


/**
 * Main workflow orchestrating the complete opening balance batch processing.
 * Processes opening balance windows sequentially with callback-based sub-workflows.
 */
@Component(id = "transaction-batch-window")
public class TransactionBatchWindowWorkflow extends Workflow<TransactionBatchWindowState> {

    private static final Logger logger = LoggerFactory.getLogger(TransactionBatchWindowWorkflow.class);

    private final ComponentClient componentClient;
    private final TaxDataRepository taxDataRepository;
    private final ProcessingConfig processingConfig;
    private final Materializer materializer;

    public TransactionBatchWindowWorkflow(
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

            .stepTimeout(TransactionBatchWindowWorkflow::notifyParentStep, Duration.ofMinutes(1))
            .defaultStepRecovery(maxRetries(0).failoverTo(TransactionBatchWindowWorkflow::errorHandlingStep))
            .build();
    }

    public record StartBatchCommand(String batchId, String taxYear, String windowId, int windowOffset, int windowLimit, String parentWorkflowId) {}

    public record BatchStatusResponse(
            String windowId,
            int windowOffset,
            int windowLimit,
            String batchId,
            String taxYear,
            TransactionBatchWindowState.ProcessingStatus status,
            Optional<String> errorMessage
    ) {}

    public record CompleteCommand(
            Optional<String> errorMessage
    ) {}

    public record CommitOffsetCommand(
            int offset
    ) {}

    @Override
    public TransactionBatchWindowState emptyState() {
        return TransactionBatchWindowState.empty();
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
        var microBatchCount = (int) Math.ceil((double) command.windowLimit / processingConfig.transactionMicrobatchLimit());
        return effects()
            .updateState(TransactionBatchWindowState.start(command.windowId(),command.windowOffset(), command.windowLimit(), microBatchCount, processingConfig.transactionMicrobatchLimit(), command.batchId(), command.taxYear(), command.parentWorkflowId()))
            .transitionTo(TransactionBatchWindowWorkflow::startStep)
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
            state.errorMessage()
        ));
    }
    public Effect<Done> offsetCommit(CommitOffsetCommand command) {
        final var state = currentState();
        if (state.status() != TransactionBatchWindowState.ProcessingStatus.RUNNING) {
            logger.error("[{}] Ignoring late offsetCommit notification", commandContext().workflowId());
            return effects().reply(Done.getInstance());
        }
        logger.debug("[{}] Callback for offsetCommit: [{}]", commandContext().workflowId(), command);

        return effects()
                .updateState(state.withMicroBatchOffset(command.offset()))
                .pause()
                .thenReply(Done.getInstance());
    }

    public Effect<Done> complete(CompleteCommand command) {
        final var state = currentState();

        if (state.status() != TransactionBatchWindowState.ProcessingStatus.RUNNING) {
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
                        .transitionTo(TransactionBatchWindowWorkflow::notifyParentStep)
                        .thenReply(Done.getInstance());
            }else{
                updatedState = updatedState.withStatus(TransactionBatchWindowState.ProcessingStatus.START);
                logger.info("[{}] Retry startStep: [{}]", commandContext().workflowId(), updatedState.retries());
                return effects()
                        .updateState(updatedState)
                        .transitionTo(TransactionBatchWindowWorkflow::startStep)
                        .thenReply(Done.getInstance());
            }

        }else{
            return effects()
                    .updateState(state.withStatus(TransactionBatchWindowState.ProcessingStatus.COMPLETED))
                    .transitionTo(TransactionBatchWindowWorkflow::notifyParentStep)
                    .thenReply(Done.getInstance());
        }
    }


    @StepName("start")
    private StepEffect startStep() {
        final var state = currentState();

        var startMicrobatchOffset = state.microBatchOffset() + 1;
        logger.info("[{}] startStep: windowOffset={}, windowLimit={}, microBatchCount={}, transPerMicroBatch={}, startMicrobatchOffset={}", commandContext().workflowId(), state.windowOffset(), state.windowLimit(), state.microBatchCount(), state.transPerMicroBatch(), startMicrobatchOffset);

        final var myWorkflowId = commandContext().workflowId();

        Flow<Integer, Done, ?> microBatchFlow =
                Flow.<Integer>create()
                    .mapAsync( 1, microBatchIndex -> {
                        var offset = state.windowOffset() + microBatchIndex * state.transPerMicroBatch();
                        logger.info("[{}] microBatchFlow: offset={}, transPerMicroBatch={}", myWorkflowId, offset, state.transPerMicroBatch());
                        return Source.fromPublisher(taxDataRepository.loadTransactionsFlux(state.taxYear(), offset, state.transPerMicroBatch()))
                                     .mapAsyncPartitioned(25, 1, t -> t.positionId().toEntityId(),  (transaction, positionEntityId) -> { //TODO mapAsync parallelism doesn't have to be transPerMicroBatch
                                         return componentClient.forEventSourcedEntity(positionEntityId)
                                                 .method(PositionEntity::processTransaction)
                                                 .invokeAsync(transaction)
                                                 .thenApply(tr -> Done.getInstance());
                                     }).toMat(Sink.ignore(),Keep.right())
                                .run(materializer)
                                .thenApply(d -> microBatchIndex);
                    }).mapAsync(1, microBatchIndex ->
                        componentClient.forWorkflow(myWorkflowId).method(TransactionBatchWindowWorkflow::offsetCommit).invokeAsync(new TransactionBatchWindowWorkflow.CommitOffsetCommand(microBatchIndex))
                    );

        //TODO add hook for stop


        var run =
        Source.range(startMicrobatchOffset, state.microBatchCount())
                .via(microBatchFlow)
                .toMat(Sink.ignore(),Keep.right())
                .run(materializer)
                .handleAsync((d, e) -> {
                    var cmd = new CompleteCommand(Optional.ofNullable(e).map(Throwable::getMessage));
                    return componentClient.forWorkflow(myWorkflowId).method(TransactionBatchWindowWorkflow::complete).invokeAsync(cmd);
                });

        logger.info("[{}] startStep streaming running!", commandContext().workflowId());
        return stepEffects()
                .updateState(state.withStatus(TransactionBatchWindowState.ProcessingStatus.RUNNING))
                .thenPause();
    }

    @StepName("start2")
    private StepEffect startStep2() {
        final var state = currentState();
//    logger.info("[{}] startStep: windowOffset={}, windowLimit={}, microBatchCount={}, transPerMicroBatch={}", commandContext().workflowId(), state.windowOffset(), state.windowLimit(), state.microBatchCount(), state.transPerMicroBatch());

        final var myWorkflowId = commandContext().workflowId();

//        var offset = state.windowOffset() + 0 * state.transPerMicroBatch();
        var offset = state.windowOffset();
//        var limit = state.microBatchCount() * state.transPerMicroBatch();
        var limit = state.windowLimit();
        var nextOffset = offset + limit;
        logger.info("[{}] microBatchFlow: offset={}, limit={}, nextOffset={}", myWorkflowId, offset, limit,nextOffset);

        Source.fromPublisher(taxDataRepository.loadTransactionsFlux(state.taxYear(), offset, limit))
                .mapAsyncPartitioned(state.transPerMicroBatch(), 1, t -> t.positionId().toEntityId(), (transaction, positionEntityId) -> {
                    return componentClient.forEventSourcedEntity(positionEntityId)
                            .method(PositionEntity::processTransaction)
                            .invokeAsync(transaction)
                            .thenApply(tr -> Done.getInstance());
                })
                .toMat(Sink.ignore(),Keep.right())
                .run(materializer)
                .handleAsync((done, throwable) -> {
                    logger.info("[{}] Batch is done: offset={}, limit={}, nextOffset={}", myWorkflowId, offset, limit,nextOffset);
                    var cmd = new CompleteCommand(Optional.ofNullable(throwable).map(Throwable::getMessage));
                    return componentClient.forWorkflow(myWorkflowId).method(TransactionBatchWindowWorkflow::complete).invokeAsync(cmd);
                });

        //TODO add hook for stop

        logger.info("[{}] startStep streaming running!", commandContext().workflowId());
        return stepEffects()
                .updateState(state.withStatus(TransactionBatchWindowState.ProcessingStatus.RUNNING))
                .thenPause();
    }



    @StepName("notify-parent")
    private StepEffect notifyParentStep() {
        var state = currentState();
        var isSuccess = state.status() == TransactionBatchWindowState.ProcessingStatus.COMPLETED;

        if (isSuccess) {
            logger.info("[{}] Successfully completed batch processing",  commandContext().workflowId());
        } else {
            logger.error("[{}] Processing failed: {}", commandContext().workflowId(), state.errorMessage());
        }

        var callbackCommand = new TransactionBatchWindowControllerWorkflow.SubWorkflowCompletedCommand(
               state.windowId(),
               state.windowLimit(),
               state.errorMessage().orElse(null)
        );

        logger.info("[{}] Notifying parent workflow {} of completion.",
                commandContext().workflowId(),
                state.parentWorkflowId());

        componentClient.forWorkflow(state.parentWorkflowId())
                .method(TransactionBatchWindowControllerWorkflow::onSubWorkflowCompleted)
                .invoke(callbackCommand);

        return stepEffects().thenEnd();
    }
    @StepName("error-handling")
    private StepEffect errorHandlingStep() {
        logger.error("errorHandlingStep for {} window!", commandContext().workflowId());
        // Error occurred during processing
        return stepEffects()
            .updateState(currentState().withError("Batch processing failed due to system error"))
            .thenTransitionTo(TransactionBatchWindowWorkflow::notifyParentStep);
    }
}