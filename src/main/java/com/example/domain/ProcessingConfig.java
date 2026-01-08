package com.example.domain;

import com.typesafe.config.Config;

/**
 * Configuration for tax processing batch operations.
 * Optimized for 5,812 TPS target with database connection constraints.
 * Loads configuration from application.conf under 'tax-processing' path.
 */
public record ProcessingConfig(
    int positionsPerWindow,          // 5,000 - opening balance positions per window

    int positionInitBatchSize,       // 500 - position entities initialized per step
    int positionsPerBatch,           // 111 - positions per batch
    int transactionWindowSize,       // 320 - transactions loaded per query window
    int maxParallelWindows,          // 3 - max concurrent window workflows
    int positionIdempotencyCacheSize, // 1000 - max number of processed transaction IDs to keep per position

     int transactionsPerWindow,
    int transactionMicrobatchLimit,

    int transactionsBatchParallelism

) {

    /**
     * Load configuration from Akka Config.
     * @param config the Config instance to load from
     * @return ProcessingConfig loaded from config
     */
    public static ProcessingConfig fromConfig(Config config) {
        var processingConfig = config.getConfig("tax-processing");
        return new ProcessingConfig(
            processingConfig.getInt("positions-per-window"),
            processingConfig.getInt("position-init-batch-size"),
            processingConfig.getInt("positions-per-batch"),
            processingConfig.getInt("transaction-window-size"),
            processingConfig.getInt("max-parallel-windows"),
            processingConfig.getInt("position-idempotency-cache-size"),

                processingConfig.getInt("transactions-per-window"),
                processingConfig.getInt("transactions-microbatch-limit"),
                processingConfig.getInt("transactions-batch-parallelism")
        );
    }

    /**
     * Default configuration for fallback.
     */
    public static ProcessingConfig defaultConfig() {
        return new ProcessingConfig(
            5000,  // positionsPerWindow
            500,   // positionInitBatchSize
            111,   // transactionMicrobatchSize
            320,   // transactionWindowSize
            3,     // maxParallelWindows
            1000,   // positionIdempotencyCacheSize,
            1000,
            100,
                25
        );
    }

    public ProcessingConfig {
        if (positionsPerWindow <= 0) {
            throw new IllegalArgumentException("Positions per window must be positive");
        }
        if (positionInitBatchSize <= 0) {
            throw new IllegalArgumentException("Position init batch size must be positive");
        }
        if (positionsPerBatch <= 0) {
            throw new IllegalArgumentException("Positions per batch size must be positive");
        }

        if (maxParallelWindows <= 0) {
            throw new IllegalArgumentException("Max parallel windows must be positive");
        }
        if (positionIdempotencyCacheSize <= 0) {
            throw new IllegalArgumentException("Position idempotency cache size must be positive");
        }
    }
}