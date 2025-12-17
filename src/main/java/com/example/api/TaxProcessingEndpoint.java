package com.example.api;

import akka.Done;
import akka.javasdk.annotations.Acl;
import akka.javasdk.annotations.http.*;
import akka.javasdk.client.ComponentClient;
import akka.javasdk.http.AbstractHttpEndpoint;
import com.example.application.OpeningBalanceBatchWorkflow;
import com.example.domain.OpeningBalanceBatchState;
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

        var command = new OpeningBalanceBatchWorkflow.StartBatchCommand(batchId, request.taxYear());

        componentClient.forWorkflow(batchId)
            .method(OpeningBalanceBatchWorkflow::start)
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
            .method(OpeningBalanceBatchWorkflow::getStatus)
            .invoke();

        return toApiStatus(status);
    }

    /**
     * Convert internal workflow status to API response format.
     */
    private BatchStatusResponse toApiStatus(OpeningBalanceBatchWorkflow.BatchStatusResponse internalStatus) {
        return new BatchStatusResponse(
            internalStatus.batchId(),
                internalStatus.taxYear(),
            internalStatus.status(),
            internalStatus.totalPositions(),
            internalStatus.totalWindows(),
            internalStatus.currentWindow(),
            internalStatus.windows(),
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
        OpeningBalanceBatchState.ProcessingStatus status,
        long totalPositions,
        int totalWindows,
        int currentWindow,
        Map<Integer,OpeningBalanceBatchState.Window> windows,
        String errorMessage
    ) {}
}