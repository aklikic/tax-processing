package com.example.infrastructure;

import com.typesafe.config.Config;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.postgresql.client.SSLMode;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Configuration for PostgreSQL R2DBC ConnectionFactory.
 * Creates a connection pool for efficient database access using Akka Config.
 */
public class DatabaseConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseConfiguration.class);

    /**
     * Creates a ConnectionFactory for PostgreSQL with connection pooling from Akka Config.
     *
     * @param config Akka Config instance
     * @return configured ConnectionFactory with pooling
     */
    public static ConnectionFactory createConnectionFactory(Config config) {
        var dbConfig = config.getConfig("tax-processing.database");

        var host = dbConfig.getString("host");
        var port = dbConfig.getInt("port");
        var database = dbConfig.getString("database");
        var username = dbConfig.getString("username");
        var password = dbConfig.getString("password");

        var monitoringDelay = dbConfig.getInt(" monitoring-delay");
        var sslEnabled = dbConfig.getBoolean("ssl-enabled");

        var poolConfig = dbConfig.getConfig("pool");
        var initialSize = poolConfig.getInt("initial-size");
        var maxSize = poolConfig.getInt("max-size");
        var maxIdleTime = poolConfig.getDuration("max-idle-time");
        var maxLifetime = poolConfig.getDuration("max-lifetime");
        var maxAcquireTime = poolConfig.getDuration("max-acquire-time");
        var maxCreateConnectionTime = poolConfig.getDuration("max-create-connection-time");

        logger.info("Creating PostgreSQL ConnectionFactory: host={}, port={}, database={}, user={}, poolSize={}",
                   host, port, database, username, maxSize);

        // Create base PostgreSQL ConnectionFactory
        var postgresqlConfig = PostgresqlConnectionConfiguration.builder()
            .host(host)
            .port(port)
            .database(database)
            .username(username)
            .password(password)
            .sslMode(sslEnabled?SSLMode.REQUIRE:SSLMode.DISABLE)
            .build();

        var connectionFactory = new PostgresqlConnectionFactory(postgresqlConfig);

        // Wrap with connection pooling
        var poolConfiguration = ConnectionPoolConfiguration.builder(connectionFactory)
            .maxIdleTime(maxIdleTime)
            .maxLifeTime(maxLifetime)
            .maxAcquireTime(maxAcquireTime)
            .maxCreateConnectionTime(maxCreateConnectionTime)
            .initialSize(initialSize)
            .maxSize(maxSize)
            .validationQuery("SELECT 1")
            .build();

        var connectionPool = new ConnectionPool(poolConfiguration);

        // Start pool metrics logging
        startPoolMetricsLogging(connectionPool, maxSize, monitoringDelay);

        return connectionPool;
    }

    /**
     * Starts periodic logging of connection pool metrics.
     * Logs pool statistics every 30 seconds at INFO level.
     *
     * @param connectionPool the connection pool to monitor
     * @param maxPoolSize maximum pool size for percentage calculations
     */
    private static void startPoolMetricsLogging(ConnectionPool connectionPool, int maxPoolSize, int delay) {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "pool-metrics-logger");
            t.setDaemon(true);
            return t;
        });

        scheduler.scheduleWithFixedDelay(() -> {
            try {
                var metrics = connectionPool.getMetrics();
                if (metrics.isPresent()) {
                    var m = metrics.get();
                    var utilizationPercent = (double) m.acquiredSize() / maxPoolSize * 100.0;

                    logger.info("R2DBC Pool Stats: " +
                        "acquired={}/{} ({}%), " +
                        "idle={}, " +
                        "pending={}, " +
                        "allocatedSize={}, " +
                        "maxAllocatedSize={}, " +
                        "maxPendingAcquireSize={}",
                        m.acquiredSize(), maxPoolSize, String.format("%.1f", utilizationPercent),
                        m.idleSize(),
                        m.pendingAcquireSize(),
                        m.allocatedSize(),
                        m.getMaxAllocatedSize(),
                        m.getMaxPendingAcquireSize()
                    );
                } else {
                    logger.debug("R2DBC Pool metrics not available");
                }
            } catch (Exception e) {
                logger.warn("Failed to log pool metrics: {}", e.getMessage());
            }
        }, delay, delay, TimeUnit.SECONDS); // Log every X seconds

        logger.info("Started R2DBC connection pool metrics logging (every {} seconds)",  delay);
    }
}