package com.example.domain;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * FIFO-based bounded cache for storing processed transaction IDs.
 * When the cache reaches its maximum size, the oldest entries are removed.
 */
public record BoundedTransactionIdCache(
    Set<String> transactionIds,
    int maxSize
) {

    public BoundedTransactionIdCache {
        if (transactionIds == null) {
            throw new IllegalArgumentException("Transaction IDs set cannot be null");
        }
        if (maxSize <= 0) {
            throw new IllegalArgumentException("Max size must be positive");
        }
        if (transactionIds.size() > maxSize) {
            throw new IllegalArgumentException("Transaction IDs set size exceeds max size");
        }
    }

    /**
     * Create an empty cache with the specified maximum size.
     */
    public static BoundedTransactionIdCache empty(int maxSize) {
        return new BoundedTransactionIdCache(new LinkedHashSet<>(), maxSize);
    }

    /**
     * Check if a transaction ID exists in the cache.
     */
    public boolean contains(String transactionId) {
        return transactionIds.contains(transactionId);
    }

    /**
     * Add a transaction ID to the cache.
     * If the cache is at capacity, removes the oldest entry first (FIFO).
     * Returns a new cache instance with the added transaction ID.
     */
    public BoundedTransactionIdCache add(String transactionId) {
        if (transactionIds.contains(transactionId)) {
            // Already exists, no change needed
            return this;
        }

        var newTransactionIds = new LinkedHashSet<>(transactionIds);

        // If at capacity, remove the oldest entry (first in LinkedHashSet)
        if (newTransactionIds.size() >= maxSize) {
            var iterator = newTransactionIds.iterator();
            if (iterator.hasNext()) {
                iterator.next();
                iterator.remove();
            }
        }

        newTransactionIds.add(transactionId);
        return new BoundedTransactionIdCache(newTransactionIds, maxSize);
    }

    /**
     * Get the number of transaction IDs currently in the cache.
     */
    public int size() {
        return transactionIds.size();
    }

    /**
     * Check if the cache is empty.
     */
    public boolean isEmpty() {
        return transactionIds.isEmpty();
    }

    /**
     * Check if the cache is at capacity.
     */
    public boolean isFull() {
        return transactionIds.size() >= maxSize;
    }
}