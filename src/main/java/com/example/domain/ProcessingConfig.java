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
    int transactionMicrobatchSize,   // 111 - positions per transaction microbatch
    int transactionWindowSize,       // 320 - transactions loaded per query window
    int maxParallelSubWorkflows,     // 45 - limited by database connection pool
    int maxParallelWindows,          // 3 - max concurrent window workflows
    int completionWindow,            // 5 - start next batch after 5 completions
    int emergencyThreshold,          // 10 - start immediately if pool drops below this
    int positionIdempotencyCacheSize // 1000 - max number of processed transaction IDs to keep per position
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
            processingConfig.getInt("transaction-microbatch-size"),
            processingConfig.getInt("transaction-window-size"),
            processingConfig.getInt("max-parallel-sub-workflows"),
            processingConfig.getInt("max-parallel-windows"),
            processingConfig.getInt("completion-window"),
            processingConfig.getInt("emergency-threshold"),
            processingConfig.getInt("position-idempotency-cache-size")
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
            45,    // maxParallelSubWorkflows
            3,     // maxParallelWindows
            5,     // completionWindow
            10,    // emergencyThreshold
            1000   // positionIdempotencyCacheSize
        );
    }

    public ProcessingConfig {
        if (positionsPerWindow <= 0) {
            throw new IllegalArgumentException("Positions per window must be positive");
        }
        if (positionInitBatchSize <= 0) {
            throw new IllegalArgumentException("Position init batch size must be positive");
        }
        if (transactionMicrobatchSize <= 0) {
            throw new IllegalArgumentException("Transaction microbatch size must be positive");
        }
        if (maxParallelSubWorkflows <= 0) {
            throw new IllegalArgumentException("Max parallel sub-workflows must be positive");
        }
        if (maxParallelWindows <= 0) {
            throw new IllegalArgumentException("Max parallel windows must be positive");
        }
        if (completionWindow <= 0 || completionWindow > maxParallelSubWorkflows) {
            throw new IllegalArgumentException("Completion window must be positive and <= max parallel workflows");
        }
        if (emergencyThreshold <= 0 || emergencyThreshold >= maxParallelSubWorkflows) {
            throw new IllegalArgumentException("Emergency threshold must be positive and < max parallel workflows");
        }
        if (positionIdempotencyCacheSize <= 0) {
            throw new IllegalArgumentException("Position idempotency cache size must be positive");
        }
    }
}