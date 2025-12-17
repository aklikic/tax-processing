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
     * Get overall processing summary statistics.
     * GET /api/processing-status/summary
     */
    @Get("/summary")
    public PositionProcessingStatusView.ProcessingSummary getProcessingSummary() {
        logger.info("Getting processing summary");

        // Since Akka SDK doesn't support complex aggregations, do them in code
        var allPositions = componentClient.forView()
            .method(PositionProcessingStatusView::getAllPositionsForSummary)
            .invoke();

        var positions = allPositions.positions();
        var totalPositions = positions.size();
        var initializedPositions = (int) positions.stream()
            .mapToInt(p -> p.initialized() ? 1 : 0)
            .sum();
        var positionsWithTransactions = (int) positions.stream()
            .mapToInt(p -> p.transactionsProcessed() > 0 ? 1 : 0)
            .sum();
        var totalTransactionsProcessed = positions.stream()
            .mapToInt(PositionProcessingStatusView.PositionStatusEntry::transactionsProcessed)
            .sum();
        var totalGainLoss = positions.stream()
            .map(PositionProcessingStatusView.PositionStatusEntry::totalGainLoss)
            .reduce(java.math.BigDecimal.ZERO, java.math.BigDecimal::add);
        var lastActivityTime = positions.stream()
            .map(PositionProcessingStatusView.PositionStatusEntry::lastTransactionTime)
            .filter(java.util.Optional::isPresent)
            .map(java.util.Optional::get)
            .max(java.time.Instant::compareTo)
            .orElse(null);

        return new PositionProcessingStatusView.ProcessingSummary(
            totalPositions,
            initializedPositions,
            positionsWithTransactions,
            totalTransactionsProcessed,
            totalGainLoss,
            lastActivityTime
        );
    }

    /**
     * Get all position processing statuses.
     * GET /api/processing-status/positions
     */
    @Get("/positions")
    public PositionProcessingStatusView.PositionStatusResult getAllPositions() {
        logger.info("Getting all position statuses");
        return componentClient.forView()
            .method(PositionProcessingStatusView::getAllPositions)
            .invoke();
    }

    /**
     * Get position processing status for a specific position.
     * GET /api/processing-status/positions/{positionId}
     */
    @Get("/positions/{positionId}")
    public PositionProcessingStatusView.PositionStatusEntry getPosition(String positionId) {
        logger.info("Getting position status for: {}", positionId);
        return componentClient.forView()
            .method(PositionProcessingStatusView::getPosition)
            .invoke(positionId);
    }

    /**
     * Get position processing statuses for a specific account.
     * GET /api/processing-status/accounts/{accountId}/positions
     */
    @Get("/accounts/{accountId}/positions")
    public PositionProcessingStatusView.PositionStatusResult getPositionsByAccount(String accountId) {
        logger.info("Getting position statuses for account: {}", accountId);
        return componentClient.forView()
            .method(PositionProcessingStatusView::getPositionsByAccount)
            .invoke(accountId);
    }

    /**
     * Get position processing statuses for a specific instrument.
     * GET /api/processing-status/instruments/{instrumentId}/positions
     */
    @Get("/instruments/{instrumentId}/positions")
    public PositionProcessingStatusView.PositionStatusResult getPositionsByInstrument(String instrumentId) {
        logger.info("Getting position statuses for instrument: {}", instrumentId);
        return componentClient.forView()
            .method(PositionProcessingStatusView::getPositionsByInstrument)
            .invoke(instrumentId);
    }

    /**
     * Get positions that have been initialized but not yet processed any transactions.
     * GET /api/processing-status/positions/unprocessed
     */
    @Get("/positions/unprocessed")
    public PositionProcessingStatusView.PositionStatusResult getUnprocessedPositions() {
        logger.info("Getting unprocessed positions");
        return componentClient.forView()
            .method(PositionProcessingStatusView::getUnprocessedPositions)
            .invoke();
    }

    /**
     * Get recently processed positions (up to specified limit).
     * GET /api/processing-status/positions/recent/{limit}
     */
    @Get("/positions/recent/{limit}")
    public PositionProcessingStatusView.PositionStatusResult getRecentlyProcessedPositions(int limit) {
        logger.info("Getting recently processed positions, limit: {}", limit);
        return componentClient.forView()
            .method(PositionProcessingStatusView::getRecentlyProcessedPositions)
            .invoke(limit);
    }

    /**
     * Get account processing summary (manual aggregation).
     * GET /api/processing-status/accounts
     */
    @Get("/accounts")
    public PositionProcessingStatusView.AccountSummaryResult getAccountSummary() {
        logger.info("Getting account processing summary");

        // Since Akka SDK doesn't support GROUP BY, aggregate manually
        var allPositions = componentClient.forView()
            .method(PositionProcessingStatusView::getAllPositionsForSummary)
            .invoke();

        var accountSummaries = allPositions.positions().stream()
            .collect(java.util.stream.Collectors.groupingBy(
                PositionProcessingStatusView.PositionStatusEntry::accountId))
            .entrySet().stream()
            .map(entry -> {
                var accountId = entry.getKey();
                var positions = entry.getValue();
                var totalPositions = positions.size();
                var totalTransactions = positions.stream()
                    .mapToInt(PositionProcessingStatusView.PositionStatusEntry::transactionsProcessed)
                    .sum();
                var totalGainLoss = positions.stream()
                    .map(PositionProcessingStatusView.PositionStatusEntry::totalGainLoss)
                    .reduce(java.math.BigDecimal.ZERO, java.math.BigDecimal::add);
                var lastActivity = positions.stream()
                    .map(PositionProcessingStatusView.PositionStatusEntry::lastTransactionTime)
                    .filter(java.util.Optional::isPresent)
                    .map(java.util.Optional::get)
                    .max(java.time.Instant::compareTo)
                    .orElse(null);

                return new PositionProcessingStatusView.AccountSummary(
                    accountId, totalPositions, totalTransactions, totalGainLoss, lastActivity
                );
            })
            .sorted(java.util.Comparator.comparing(PositionProcessingStatusView.AccountSummary::accountId))
            .toList();

        return new PositionProcessingStatusView.AccountSummaryResult(accountSummaries);
    }

    /**
     * Get positions that appear incomplete (haven't been updated recently).
     * GET /api/processing-status/positions/incomplete/{hours}
     */
    @Get("/positions/incomplete/{hours}")
    public PositionProcessingStatusView.PositionStatusResult getIncompletePositions(int hours) {
        var cutoffTime = Instant.now().minus(hours, ChronoUnit.HOURS);

        logger.info("Getting incomplete positions (cutoff: {} hours ago)", hours);

        // Manual filtering since Akka SDK doesn't support date comparisons
        var allPositions = componentClient.forView()
            .method(PositionProcessingStatusView::getAllPositionsForSummary)
            .invoke();

        var incompletePositions = allPositions.positions().stream()
            .filter(p -> !p.initialized() || p.lastUpdated().isBefore(cutoffTime))
            .sorted(java.util.Comparator.comparing(PositionProcessingStatusView.PositionStatusEntry::lastUpdated))
            .toList();

        return new PositionProcessingStatusView.PositionStatusResult(incompletePositions);
    }

    /**
     * Check if processing appears to be complete.
     * Returns a simple completion status.
     * GET /api/processing-status/completion-check
     */
    @Get("/completion-check")
    public CompletionStatus checkCompletion() {
        logger.info("Checking processing completion status");

        // Get all positions for manual aggregation
        var allPositions = componentClient.forView()
            .method(PositionProcessingStatusView::getAllPositionsForSummary)
            .invoke();

        var positions = allPositions.positions();
        var totalPositions = positions.size();
        var initializedPositions = (int) positions.stream()
            .mapToInt(p -> p.initialized() ? 1 : 0)
            .sum();
        var positionsWithTransactions = (int) positions.stream()
            .mapToInt(p -> p.transactionsProcessed() > 0 ? 1 : 0)
            .sum();
        var totalTransactionsProcessed = positions.stream()
            .mapToInt(PositionProcessingStatusView.PositionStatusEntry::transactionsProcessed)
            .sum();
        var lastActivityTime = positions.stream()
            .map(PositionProcessingStatusView.PositionStatusEntry::lastTransactionTime)
            .filter(java.util.Optional::isPresent)
            .map(java.util.Optional::get)
            .max(java.time.Instant::compareTo)
            .orElse(null);

        // Check if there are recent incomplete positions
        var cutoffTime = Instant.now().minus(30, ChronoUnit.MINUTES); // 30 minutes ago
        var incompletePositions = positions.stream()
            .filter(p -> !p.initialized() || p.lastUpdated().isBefore(cutoffTime))
            .toList();

        boolean isComplete = totalPositions > 0 &&
                           initializedPositions == totalPositions &&
                           incompletePositions.isEmpty();

        return new CompletionStatus(
            isComplete,
            totalPositions,
            initializedPositions,
            positionsWithTransactions,
            totalTransactionsProcessed,
            incompletePositions.size(),
            lastActivityTime
        );
    }

    public record CompletionStatus(
        boolean isComplete,
        int totalPositions,
        int initializedPositions,
        int positionsWithTransactions,
        int totalTransactionsProcessed,
        int incompletePositions,
        Instant lastActivity
    ) {}
}