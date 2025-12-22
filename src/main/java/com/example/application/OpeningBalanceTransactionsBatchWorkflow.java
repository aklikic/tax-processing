package com.example.application;

import akka.Done;
import akka.javasdk.annotations.Component;
import akka.javasdk.client.ComponentClient;
import akka.javasdk.workflow.Workflow;
import akka.javasdk.annotations.StepName;
import com.example.domain.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Sub-workflow that processes transactions for a batch of already-initialized positions.
 *
 * This workflow:
 * 1. Loads transactions for the positions in chronological order (one window at a time)
 * 2. Processes each transaction through the corresponding PositionEntity using async calls
 * 3. Continues to next window until all transactions are processed
 * 4. Logs completion for now (will notify parent workflow when implemented)
 *
 * Note: PositionEntity instances must already be initialized before this workflow starts.
 */
@Component(id = "opening-balance-transactions-batch")
public class OpeningBalanceTransactionsBatchWorkflow extends Workflow<OpeningBalanceTransactionsBatchState> {

    private static final Logger logger = LoggerFactory.getLogger(OpeningBalanceTransactionsBatchWorkflow.class);

    private final ComponentClient componentClient;
    private final TaxDataRepository dataRepository;
    private final ProcessingConfig processingConfig;

    public OpeningBalanceTransactionsBatchWorkflow(
            ComponentClient componentClient,
            TaxDataRepository dataRepository,
            ProcessingConfig processingConfig) {
        this.componentClient = componentClient;
        this.dataRepository = dataRepository;
        this.processingConfig = processingConfig;
    }

    @Override
    public WorkflowSettings settings() {
        return WorkflowSettings.builder()
            .defaultStepTimeout(Duration.ofSeconds(30))
            .stepTimeout(OpeningBalanceTransactionsBatchWorkflow::processTransactionsStep, Duration.ofMinutes(5))
            .defaultStepRecovery(maxRetries(2).failoverTo(OpeningBalanceTransactionsBatchWorkflow::errorStep))
            .build();
    }

    /**
     * Start processing transactions for a batch of positions.
     * Positions must already be initialized.
     */
    public Effect<Done> start(StartCommand command) {
        var initialState = OpeningBalanceTransactionsBatchState.init(
            command.batchId(),
            command.taxYear(),
            command.positionIds(),
            command.parentWorkflowId()
        );

        logger.info("[{}] Starting batch processing", commandContext().workflowId());

        return effects()
            .updateState(initialState.withStatus(OpeningBalanceTransactionsBatchState.ProcessingStatus.PROCESSING_TRANSACTIONS))
            .transitionTo(OpeningBalanceTransactionsBatchWorkflow::processTransactionsStep)
            .thenReply(Done.getInstance());
    }

    /**
     * Get the current processing status.
     */
    public Effect<BatchStatus> getStatus() {
        if (currentState() == null) {
            return effects().error("Batch not found");
        }

        return effects().reply(new BatchStatus(
            currentState().batchId(),
            currentState().status(),
            currentState().processedTransactions(),
            currentState().totalPositions(),
            currentState().errorMessage()
        ));
    }

    @StepName("process-transactions")
    private StepEffect processTransactionsStep() {

        // Use injected processingConfig
        var positionIds = currentState().positionIds();

        logger.info("[{}] Processing transactions in offset={}",
            commandContext().workflowId(), currentState().processedTransactions());

        // Load one window of transactions
        var transactions = dataRepository.loadTransactionsForPositions(
            positionIds,
            currentState().taxYear(),
            currentState().processedTransactions(),
            processingConfig.transactionWindowSize()
        );

        if (transactions.isEmpty()) {
            logger.info("[{}] No more transactions found",
                commandContext().workflowId());

            // No more transactions - mark as completed
            var completedState = currentState()
                .withStatus(OpeningBalanceTransactionsBatchState.ProcessingStatus.COMPLETED);

            return stepEffects()
                .updateState(completedState)
                .thenTransitionTo(OpeningBalanceTransactionsBatchWorkflow::notifyParentStep);
        }

        logger.info("[{}] Processing {} transactions in offset={}",
            commandContext().workflowId(),
            transactions.size(), currentState().processedTransactions());

        // Process each transaction in this window using async calls
        var processingFutures = transactions.stream()
            .map(transaction -> {
                var positionEntityId = transaction.positionId().toEntityId();
                return componentClient.forEventSourcedEntity(positionEntityId)
                    .method(PositionEntity::processTransaction)
                    .invokeAsync(transaction);
            })
            .toList();

        // Wait for all transactions in this window to complete
        var allTransactionsProcessed = CompletableFuture.allOf(
            processingFutures.toArray(new CompletableFuture[0])
        );

        // Materialize the futures - wait for completion
        allTransactionsProcessed.join();

        logger.info("[{}] Completed processing {} transactions in offset={}",
            commandContext().workflowId(),
            transactions.size(), currentState().processedTransactions());

        // Update state and continue to next window
        var updatedState = currentState()
            .withProcessedTransactions(currentState().processedTransactions() + transactions.size());

        return stepEffects()
            .updateState(updatedState)
            .thenTransitionTo(OpeningBalanceTransactionsBatchWorkflow::processTransactionsStep);

    }

    @StepName("notify-parent")
    private StepEffect notifyParentStep() {
        var state = currentState();
        var isSuccess = state.status() == OpeningBalanceTransactionsBatchState.ProcessingStatus.COMPLETED;

        if (isSuccess) {
            logger.info("[{}] Successfully completed batch processing. ProcessedTransactions={}",
               commandContext().workflowId(), state.processedTransactions());
        } else {
            logger.error("[{}] Processing failed: {} (processedTransactions={})",
                commandContext().workflowId(), state.errorMessage(), state.processedTransactions());
        }

        var callbackCommand = new BatchWindowWorkflow.SubWorkflowCompletedCommand(
            commandContext().workflowId(),
            state.processedTransactions(),
            state.totalPositions(),
            state.status(),
            state.errorMessage()
        );

        logger.info("[{}] Notifying parent workflow {} of completion.",
                commandContext().workflowId(),
                state.parentWorkflowId());

        componentClient.forWorkflow(state.parentWorkflowId())
            .method(BatchWindowWorkflow::onSubWorkflowCompleted)
            .invoke(callbackCommand);

        return stepEffects().thenEnd();
    }

    @StepName("error")
    private StepEffect errorStep() {
        logger.error("[{}] Workflow failed with unrecoverable error",
            commandContext().workflowId());

        // Final error state - log error and end
        return stepEffects()
            .updateState(currentState().withError("Workflow failed with unrecoverable error"))
            .thenTransitionTo(OpeningBalanceTransactionsBatchWorkflow::notifyParentStep);
    }


    /**
     * Command to start processing a batch with callback to parent workflow.
     */
    public record StartCommand(
        String batchId,
        String taxYear,
        List<PositionId> positionIds,
        String parentWorkflowId
    ) {}

    /**
     * Status response for monitoring.
     */
    public record BatchStatus(
        String batchId,
        OpeningBalanceTransactionsBatchState.ProcessingStatus status,
        int processedTransactions,
        int totalPositions,
        String errorMessage
    ) {}

}