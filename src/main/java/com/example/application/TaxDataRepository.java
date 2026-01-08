package com.example.application;

import com.example.domain.OpeningBalance;
import com.example.domain.PositionId;
import com.example.domain.Transaction;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.util.List;

/**
 * Repository interface for accessing tax processing data from SQL database.
 * Provides deterministic, paginated access to opening balances and transactions.
 *
 * Implementation should ensure:
 * - Consistent ordering for deterministic batching
 * - Efficient pagination with LIMIT/OFFSET
 * - Connection pooling and proper resource management
 */
public interface TaxDataRepository {

    /**
     * Load a batch of opening balances in deterministic order.
     * Results are ordered by account_id ASC, instrument_id ASC for consistent pagination.
     *
     * @param taxYear the tax year to process (e.g., "2023")
     * @param offset the starting offset (0-based)
     * @param limit the maximum number of records to return
     * @return list of opening balances, may be empty if offset exceeds data
     */
    List<OpeningBalance> loadOpeningBalancesBatch(String taxYear, int offset, int limit);

    /**
     * Load transactions for specific positions within a tax year.
     * Results are ordered by date_time ASC, transaction_id ASC for chronological processing.
     *
     * @param positionIds list of position IDs to query transactions for
     * @param taxYear the tax year to query
     * @param offset the starting offset for pagination (0-based)
     * @param limit the maximum number of transactions to return
     * @return list of transactions ordered chronologically
     */
    List<Transaction> loadTransactionsForPositions(List<PositionId> positionIds, String taxYear, int offset, int limit);

    /**
     * Get the total count of opening balances for a tax year.
     * Used for calculating total number of batches needed.
     *
     * @param taxYear the tax year to count
     * @return total number of opening balance records
     */
    long countOpeningBalances(String taxYear);

    /**
     * Get the total count of transactions for specific positions within a tax year.
     * Used for progress tracking and window sizing.
     *
     * @param positionIds list of position IDs to count transactions for
     * @param taxYear the tax year to query
     * @return total number of transaction records
     */
    long countTransactionsForPositions(List<PositionId> positionIds, String taxYear);

    /**
     * Load a batch of opening balances in deterministic order (reactive version).
     * Results are ordered by account_id ASC, instrument_id ASC for consistent pagination.
     *
     * @param taxYear the tax year to process (e.g., "2023")
     * @param offset the starting offset (0-based)
     * @param limit the maximum number of records to return
     * @return Flux of opening balances, may be empty if offset exceeds data
     */
    Flux<OpeningBalance> loadOpeningBalancesBatchFlux(String taxYear, int offset, int limit);

    /**
     * Load transactions for specific positions within a tax year (reactive version).
     * Results are ordered by date_time ASC, transaction_id ASC for chronological processing.
     *
     * @param positionIds list of position IDs to query transactions for
     * @param taxYear the tax year to query
     * @param offset the starting offset for pagination (0-based)
     * @param limit the maximum number of transactions to return
     * @return Flux of transactions ordered chronologically
     */
    Flux<Transaction> loadTransactionsForPositionsFlux(List<PositionId> positionIds, String taxYear, int offset, int limit);

    Flux<Transaction> loadTransactionsFlux(String taxYear, int offset, int limit);

    /**
     * Load transactions for positions determined by subquery with offset/limit.
     * This method first queries opening balances with the given offset/limit to get position IDs,
     * then loads transactions for those positions. Useful for processing transactions in chunks
     * corresponding to opening balance windows.
     *
     * @param taxYear the tax year to query
     * @param positionOffset the starting offset for position selection (0-based)
     * @param positionLimit the maximum number of positions to select
     * @param transactionOffset the starting offset for transaction pagination (0-based)
     * @param transactionLimit the maximum number of transactions to return
     * @return Flux of transactions for the selected positions, ordered chronologically
     */
    Flux<Transaction> loadTransactionsForPositionWindow(String taxYear, int positionOffset, int positionLimit,
                                                       int transactionOffset, int transactionLimit);

    /**
     * Count transactions for positions determined by subquery with offset/limit.
     * This method first queries opening balances with the given offset/limit to get position IDs,
     * then counts transactions for those positions.
     *
     * @param taxYear the tax year to query
     * @param positionOffset the starting offset for position selection (0-based)
     * @param positionLimit the maximum number of positions to select
     * @return total number of transactions for the selected positions
     */
    long countTransactionsForPositionWindow(String taxYear, int positionOffset, int positionLimit);

    /**
     * Get the total count of opening balances for a tax year (reactive version).
     * Used for calculating total number of batches needed.
     *
     * @param taxYear the tax year to count
     * @return Mono of total number of opening balance records
     */
    Mono<Long> countOpeningBalancesMono(String taxYear);

    /**
     * Get the total count of transactions for specific positions within a tax year (reactive version).
     * Used for progress tracking and window sizing.
     *
     * @param positionIds list of position IDs to count transactions for
     * @param taxYear the tax year to query
     * @return Mono of total number of transaction records
     */
    Mono<Long> countTransactionsForPositionsMono(List<PositionId> positionIds, String taxYear);

    Mono<Long> countTransactionsMono(String taxYear);
}