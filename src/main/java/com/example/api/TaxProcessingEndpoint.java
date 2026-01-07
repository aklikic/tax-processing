package com.example.api;

import akka.Done;
import akka.javasdk.annotations.Acl;
import akka.javasdk.annotations.http.*;
import akka.javasdk.client.ComponentClient;
import akka.javasdk.http.AbstractHttpEndpoint;
import com.example.application.BatchControllerWorkflow;
import com.example.application.TransactionBatchWindowControllerWorkflow;
import com.example.domain.BatchControllerState;
import com.example.domain.TransactionBatchWindowControllerState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * HTTP endpoint for tax processing batch operations.
 * Provides REST API for starting and monitoring opening balance batch processing.
 */
@HttpEndpoint("/tax-processing")
@Acl(allow = @Acl.Matcher(principal = Acl.Principal.ALL))
public class TaxProcessingEndpoint extends AbstractHttpEndpoint {

    private static final Logger logger = LoggerFactory.getLogger(TaxProcessingEndpoint.class);

    private final ComponentClient componentClient;

    public TaxProcessingEndpoint(ComponentClient componentClient) {
        this.componentClient = componentClient;
    }

    /**
     * Start opening balance batch processing for a specific tax year.
     *
     * POST /tax-processing/batches/{batchId}/start
     * Content-Type: application/json
     *
     * Request body:
     * {
     *   "taxYear": "2023"
     * }
     */
    @Post("/batches/{batchId}/start")
    public BatchStartResponse startBatch(String batchId, StartBatchRequest request) {
        logger.info("Starting tax processing batch: batchId={}, taxYear={}", batchId, request.taxYear());

        var command = new BatchControllerWorkflow.StartBatchCommand(batchId, request.taxYear());

        componentClient.forWorkflow(batchId)
            .method(BatchControllerWorkflow::start)
            .invoke(command);

        logger.info("Successfully started batch processing: batchId={}", batchId);

        return new BatchStartResponse(
            batchId,
            request.taxYear(),
            "Batch processing started successfully"
        );
    }

    /**
     * Get the current status of a batch processing operation.
     *
     * GET /tax-processing/batches/{batchId}/status
     */
    @Get("/batches/{batchId}/status")
    public BatchStatusResponse getBatchStatus(String batchId) {
        logger.debug("Getting batch status: batchId={}", batchId);

        var status = componentClient.forWorkflow(batchId)
            .method(BatchControllerWorkflow::getStatus)
            .invoke();

        return toApiStatus(status);
    }

    /**
     * Start transaction batch processing for a specific tax year.
     *
     * POST /tax-processing/transaction-batches/{batchId}/start
     * Content-Type: application/json
     *
     * Request body:
     * {
     *   "taxYear": "2023"
     * }
     */
    @Post("/transaction-batches/{batchId}/start")
    public TransactionBatchStartResponse startTransactionBatch(String batchId, StartBatchRequest request) {
        logger.info("Starting transaction batch processing: batchId={}, taxYear={}", batchId, request.taxYear());

        var command = new TransactionBatchWindowControllerWorkflow.StartBatchCommand(batchId, request.taxYear());

        componentClient.forWorkflow(batchId)
            .method(TransactionBatchWindowControllerWorkflow::start)
            .invoke(command);

        logger.info("Successfully started transaction batch processing: batchId={}", batchId);

        return new TransactionBatchStartResponse(
            batchId,
            request.taxYear(),
            "Transaction batch processing started successfully"
        );
    }

    /**
     * Get the current status of a transaction batch processing operation.
     *
     * GET /tax-processing/transaction-batches/{batchId}/status
     */
    @Get("/transaction-batches/{batchId}/status")
    public TransactionBatchStatusResponse getTransactionBatchStatus(String batchId) {
        logger.debug("Getting transaction batch status: batchId={}", batchId);

        var status = componentClient.forWorkflow(batchId)
            .method(TransactionBatchWindowControllerWorkflow::getStatus)
            .invoke();

        return toTransactionApiStatus(status);
    }

    /**
     * Convert internal workflow status to API response format.
     */
    private BatchStatusResponse toApiStatus(BatchControllerWorkflow.BatchStatusResponse internalStatus) {
        return new BatchStatusResponse(
            internalStatus.batchId(),
                internalStatus.taxYear(),
            internalStatus.status(),
            internalStatus.totalPositions(),
            internalStatus.totalWindows(),
            internalStatus.windowStatuses(),
            internalStatus.errorMessage()
        );
    }

    /**
     * Convert internal transaction workflow status to API response format.
     */
    private TransactionBatchStatusResponse toTransactionApiStatus(TransactionBatchWindowControllerWorkflow.BatchStatusResponse internalStatus) {
        return new TransactionBatchStatusResponse(
            internalStatus.batchId(),
            internalStatus.taxYear(),
            internalStatus.status(),
            internalStatus.totalTransactions(),
            internalStatus.windowCount(),
            internalStatus.windowStatuses(),
            internalStatus.completedTransactions(),
            internalStatus.errorMessage()
        );
    }

    // Request/Response Records

    /**
     * Request to start batch processing.
     */
    public record StartBatchRequest(
        String taxYear
    ) {}

    /**
     * Response for batch start operation.
     */
    public record BatchStartResponse(
        String batchId,
        String taxYear,
        String message
    ) {}

    /**
     * Response for batch status query.
     */
    public record BatchStatusResponse(
        String batchId,
        String taxYear,
        BatchControllerState.ProcessingStatus status,
        long totalPositions,
        int totalWindows,
        Map<String, BatchControllerState.WindowStatus> windowStatuses,
        String errorMessage
    ) {}

    /**
     * Response for transaction batch start operation.
     */
    public record TransactionBatchStartResponse(
        String batchId,
        String taxYear,
        String message
    ) {}

    /**
     * Response for transaction batch status query.
     */
    public record TransactionBatchStatusResponse(
        String batchId,
        String taxYear,
        TransactionBatchWindowControllerState.ProcessingStatus status,
        long totalTransactions,
        int windowCount,
        Map<String, TransactionBatchWindowControllerState.WindowStatus> windowStatuses,
        long completedTransactions,
        String errorMessage
    ) {}
}