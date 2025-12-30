package com.example.api;

import akka.javasdk.annotations.Acl;
import akka.javasdk.annotations.http.Get;
import akka.javasdk.annotations.http.HttpEndpoint;
import akka.javasdk.client.ComponentClient;
import akka.javasdk.http.AbstractHttpEndpoint;
import com.example.application.PositionProcessingStatusView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * HTTP Endpoint exposing position processing status queries.
 * Provides REST API access to the PositionProcessingStatusView.
 */
@HttpEndpoint("/api/processing-status")
@Acl(allow = @Acl.Matcher(principal = Acl.Principal.ALL))
public class PositionProcessingStatusEndpoint extends AbstractHttpEndpoint {

    private static final Logger logger = LoggerFactory.getLogger(PositionProcessingStatusEndpoint.class);

    private final ComponentClient componentClient;

    public PositionProcessingStatusEndpoint(ComponentClient componentClient) {
        this.componentClient = componentClient;
    }

    /**
     * Get total count of all positions being tracked.
     */
    @Get("/positions/count")
    public PositionProcessingStatusView.PositionsProcessingTotalCount getAllPositionsCount() {
        logger.info("Getting total positions count");
        return componentClient
            .forView()
            .method(PositionProcessingStatusView::getAllPositionsCount)
            .invoke();
    }

    /**
     * Get total count of positions for a specific account.
     */
    @Get("/accounts/{accountId}/positions/count")
    public PositionProcessingStatusView.PositionsProcessingTotalCount getPositionsByAccountCount(String accountId) {
        logger.info("Getting positions count for account: {}", accountId);
        return componentClient
            .forView()
            .method(PositionProcessingStatusView::getPositionsByAccountCount)
            .invoke(accountId);
    }

    /**
     * Get processing status for a specific position.
     */
    @Get("/positions/{positionId}")
    public PositionProcessingStatusView.PositionStatusEntry getPosition(String positionId) {
        logger.info("Getting status for position: {}", positionId);
        return componentClient
            .forView()
            .method(PositionProcessingStatusView::getPosition)
            .invoke(positionId);
    }

    /**
     * Get count of positions that have been initialized but not yet processed any transactions.
     */
    @Get("/positions/unprocessed/count")
    public PositionProcessingStatusView.PositionsProcessingTotalCount getUnprocessedPositionsCount() {
        logger.info("Getting unprocessed positions count");
        return componentClient
            .forView()
            .method(PositionProcessingStatusView::getUnprocessedPositionsCount)
            .invoke();
    }

    /**
     * Get count of positions with specific transaction count.
     */
    @Get("/positions/transactions/{count}/count")
    public PositionProcessingStatusView.PositionsProcessingTotalCount getPositionsCountByTransactionCount(int count) {
        logger.info("Getting positions count by transaction count: {}", count);
        return componentClient
            .forView()
            .method(PositionProcessingStatusView::getPositionsCountByTransactionCount)
            .invoke(count);
    }
}