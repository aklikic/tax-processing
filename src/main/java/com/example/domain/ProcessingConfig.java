package com.example.domain;

import com.typesafe.config.Config;

import java.time.Duration;

public record ProcessingConfig(
    int positionNumberPerWindow,
    int positionMaxParallelWindows,
    int positionsMaxCompletedWindowsToKeepInState,

    int positionIdempotencyCacheSize,

    int transactionsBatchLimit,
    int transactionsBatchParallelism,

    Duration transactionsBatchProcessingTimeout

) {

    public static ProcessingConfig fromConfig(Config config) {
        var processingConfig = config.getConfig("tax-processing");
        return new ProcessingConfig(
            processingConfig.getInt("position-number-per-window"),
            processingConfig.getInt("position-max-parallel-windows"),
            processingConfig.getInt("position-max-completed-windows-to-keep-in-state"),
            processingConfig.getInt("position-idempotency-cache-size"),
            processingConfig.getInt("transactions-batch-limit"),
            processingConfig.getInt("transactions-batch-parallelism"),
            processingConfig.getDuration("transactions-batch-running-timeout")
        );
    }
}