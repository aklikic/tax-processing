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
     * Get the total count of opening balances for a tax year (reactive version).
     * Used for calculating total number of batches needed.
     *
     * @param taxYear the tax year to count
     * @return Mono of total number of opening balance records
     */
    Mono<Long> countOpeningBalancesMono(String taxYear);

    /**
     * Get the total count of opening balances for a tax year.
     * Used for calculating total number of batches needed.
     *
     * @param taxYear the tax year to count
     * @return total number of opening balance records
     */
    long countOpeningBalances(String taxYear);

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

    Mono<Long> countTransactionsForPositionWindowMono(String taxYear, int positionOffset, int positionLimit);
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


}