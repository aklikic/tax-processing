package com.example.api;

import akka.Done;
import akka.http.javadsl.model.HttpResponse;
import akka.javasdk.annotations.Acl;
import akka.javasdk.annotations.http.*;
import akka.javasdk.client.ComponentClient;
import akka.javasdk.http.AbstractHttpEndpoint;
import akka.javasdk.http.HttpResponses;
import com.example.application.PositionBatchControllerWorkflow;
import com.example.application.PositionBatchWindowWorkflow;
import com.example.domain.PositionBatchControllerState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
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
     * Start position batch processing for a specific tax year.
     *
     * POST /tax-processing/position-batches/{batchId}/start
     * Content-Type: application/json
     *
     * Request body:
     * {
     *   "taxYear": "2023"
     * }
     */
    @Post("/position-batches/{batchId}/start")
    public PositionBatchStartResponse startPositionBatch(String batchId, StartBatchRequest request) {
        logger.info("Starting position batch processing: batchId={}, taxYear={}", batchId, request.taxYear());

        var command = new PositionBatchControllerWorkflow.StartBatchCommand(batchId, request.taxYear());

        componentClient.forWorkflow(batchId)
            .method(PositionBatchControllerWorkflow::start)
            .invoke(command);

        logger.info("Successfully started position batch processing: batchId={}", batchId);

        return new PositionBatchStartResponse(
            batchId,
            request.taxYear(),
            "Position batch processing started successfully"
        );
    }

    /**
     * Get the current status of a position batch processing operation.
     *
     * GET /tax-processing/position-batches/{batchId}/status
     */
    @Get("/position-batches/{batchId}/status")
    public PositionBatchStatusResponse getPositionBatchStatus(String batchId) {
        logger.debug("Getting position batch status: batchId={}", batchId);

        var status = componentClient.forWorkflow(batchId)
            .method(PositionBatchControllerWorkflow::getStatus)
            .invoke();

        return toPositionApiStatus(status);
    }

    @Post("/position-batches/{batchId}/window/{windowId}/running-timeout-trigger")
    public HttpResponse runningTimeoutTriggerForPositionBatchWindow(String batchId, String windowId) {
        logger.info("Trigger timeout for position batch window: batchId={}, windowId={}", batchId, windowId);

        var positionBatchWindowWorkflowId = new PositionBatchControllerState.BatchWindowWorkflowId(batchId, windowId);

        componentClient.forWorkflow(positionBatchWindowWorkflowId.serialize())
                .method(PositionBatchWindowWorkflow::runningTimeout)
                .invoke();

        logger.info("Successfully triggered position batch window timeout: batchId={}, windowId={}", batchId, windowId);

        return HttpResponses.ok();
    }


    /**
     * Convert internal position workflow status to API response format.
     */
    private PositionBatchStatusResponse toPositionApiStatus(PositionBatchControllerWorkflow.BatchStatusResponse internalStatus) {
        return new PositionBatchStatusResponse(
            internalStatus.batchId(),
            internalStatus.taxYear(),
            internalStatus.status(),
            internalStatus.totalPositions(),
            internalStatus.totalWindows(),
            internalStatus.runningWindowIds(),
            internalStatus.completedWindows(),
            internalStatus.completedPositions(),
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
     * Response for position batch start operation.
     */
    public record PositionBatchStartResponse(
        String batchId,
        String taxYear,
        String message
    ) {}

    /**
     * Response for position batch status query.
     */
    public record PositionBatchStatusResponse(
        String batchId,
        String taxYear,
        PositionBatchControllerState.ProcessingStatus status,
        long totalPositions,
        int totalWindows,
        List<String> runningWindowIds,
        int completedWindows,
        long completedPositions,
        String errorMessage
    ) {}
}