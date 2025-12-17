package com.example.infrastructure;

import com.typesafe.config.Config;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

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

        return new ConnectionPool(poolConfiguration);
    }
}