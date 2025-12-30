package com.example;

import akka.javasdk.DependencyProvider;
import akka.javasdk.ServiceSetup;
import akka.javasdk.annotations.Setup;
import com.example.application.TaxDataRepository;
import com.example.domain.ProcessingConfig;
import com.example.infrastructure.MockTaxDataRepository;
import com.example.infrastructure.PostgreSQLTaxDataRepository;
import com.example.infrastructure.DatabaseConfiguration;
import com.typesafe.config.Config;
import io.r2dbc.spi.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service bootstrap and dependency injection setup for the tax processing application.
 * Configures dependencies based on environment and application configuration.
 */
@Setup
public class Bootstrap implements ServiceSetup {

    private static final Logger logger = LoggerFactory.getLogger(Bootstrap.class);

    private final Config config;

    public Bootstrap(Config config) {
        this.config = config;
    }

    @Override
    public void onStartup() {
        logger.info("Tax Processing Service starting up");
        logger.info("config: {}", config);
    }

    @Override
    public DependencyProvider createDependencyProvider() {
        // Parse config once at startup
        var processingConfig = ProcessingConfig.fromConfig(config);

        // Create database repository based on configuration
        var taxDataRepository = createTaxDataRepository();

        return new DependencyProvider() {
            @Override
            public <T> T getDependency(Class<T> clazz) {
                if (clazz.equals(TaxDataRepository.class)) {
                    return (T) taxDataRepository;
                } else if (clazz.equals(ProcessingConfig.class)) {
                    return (T) processingConfig;
                } else {
                    throw new IllegalArgumentException("Unknown dependency type: " + clazz);
                }
            }
        };
    }

    private TaxDataRepository createTaxDataRepository() {
        var useDatabase = config.hasPath("tax-processing.database.enable") &&
                         config.getBoolean("tax-processing.database.enable");

        if (useDatabase) {
            logger.info("Initializing PostgreSQL database repository");
            var connectionFactory = DatabaseConfiguration.createConnectionFactory(config);
            return new PostgreSQLTaxDataRepository(connectionFactory);
        } else {
            logger.info("Using mock repository with default test data");
            return MockTaxDataRepository.withDefaultData();
        }
    }
}