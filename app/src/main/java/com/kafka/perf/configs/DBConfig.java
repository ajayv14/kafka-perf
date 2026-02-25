package com.kafka.perf.configs;

import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * DBConfig - Centralized database connection management.
 * 
 * Manages HikariCP connection pool initialization, verification, and connection retrieval
 * with retry logic. Shared by PostgresSinkConsumer and FaultInjectorConsumer.
 */
public class DBConfig {

    private static final Logger logger = LoggerFactory.getLogger(DBConfig.class);

    private HikariDataSource dataSource;
    private final String consumerName;

    public DBConfig(String consumerName) {
        this.consumerName = consumerName;
    }

    /**
     * Initialize HikariCP connection pool with optimized settings
     */
    public void initializeConnectionPool(KafkaConsumerConfig config) throws Exception {
        log("Initializing HikariCP connection pool...");
        
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(config.dbUrl);
        hikariConfig.setUsername(config.dbUser);
        hikariConfig.setPassword(config.dbPassword);
        
        // Connection pool settings
        hikariConfig.setMaximumPoolSize(config.dbConnectionPoolSize);
        hikariConfig.setMinimumIdle(2); // Keep minimum 2 idle connections
        hikariConfig.setConnectionTimeout(10000); // 10s timeout for acquiring connection
        hikariConfig.setIdleTimeout(600000); // 10 minutes idle timeout
        hikariConfig.setMaxLifetime(1800000); // 30 minutes max lifetime
        hikariConfig.setAutoCommit(true); // Auto-commit for simple writes
        
        // Connection test query for health checks
        hikariConfig.setConnectionTestQuery("SELECT 1");
        
        dataSource = new HikariDataSource(hikariConfig);
        log("✓ Connection pool initialized (max size: " + config.dbConnectionPoolSize + ")");
    }

    /**
     * Verify PostgreSQL connection and table existence
     */
    public void verifyDatabaseConnection(KafkaConsumerConfig config) throws Exception {
        log("Verifying database connection...");
        log("Attempting to connect to: " + config.dbUrl);
        
        int maxRetries = 15;
        int retryCount = 0;
        long backoffMs = 300;
        long verifyStartTime = System.currentTimeMillis();
        
        while (retryCount < maxRetries) {
            try (Connection conn = dataSource.getConnection()) {
                long elapsedMs = System.currentTimeMillis() - verifyStartTime;
                log("✓ Database connection successful (took " + elapsedMs + "ms)");
                log("Database URL: " + config.dbUrl);
                log("Table: " + config.dbSinkTable);
                log("PostgreSQL version: " + conn.getMetaData().getDatabaseProductVersion());
                log("Connection pool ready");
                return;
            } catch (SQLException e) {
                retryCount++;
                if (retryCount >= maxRetries) {
                    logErr("✗ Database connection failed after " + maxRetries + " attempts");
                    logErr("Error: " + e.getMessage());
                    logErr("Ensure PostgreSQL is running at: " + config.dbUrl);
                    logErr("Credentials - User: " + config.dbUser + " (password configured)");
                    throw e;
                }
                long elapsedMs = System.currentTimeMillis() - verifyStartTime;
                logger.error("[{}] Connection attempt {}/{} failed ({}ms elapsed): {}",
                    consumerName, retryCount, maxRetries, elapsedMs, e.getMessage());
                logger.error("[{}] Retrying in {}ms...", consumerName, backoffMs);
                try {
                    Thread.sleep(backoffMs);
                    backoffMs = Math.min(backoffMs * 2, 5000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new Exception("Connection verification interrupted", ie);
                }
            }
        }
    }

    /**
     * Get database connection from pool with retry logic
     */
    public Connection getConnection() throws SQLException {
        int maxRetries = 3;
        int retryCount = 0;
        long backoffMs = 100;
        
        while (retryCount < maxRetries) {
            try {
                return dataSource.getConnection();
            } catch (SQLException e) {
                retryCount++;
                if (retryCount >= maxRetries) {
                    throw new SQLException("Failed to get connection from pool after " + maxRetries + " attempts: " + e.getMessage(), e);
                }
                logger.error("[{}] Failed to get pooled connection, attempt {}/{}: {}",
                    consumerName, retryCount, maxRetries, e.getMessage());
                try {
                    Thread.sleep(backoffMs);
                    backoffMs = Math.min(backoffMs * 2, 500);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new SQLException("Connection retry interrupted", ie);
                }
            }
        }
        throw new SQLException("Failed to get connection from pool");
    }

    /**
     * Close the connection pool
     */
    public void close() {
        if (dataSource != null && !dataSource.isClosed()) {
            log("Closing connection pool...");
            dataSource.close();
        }
    }

    /**
     * Check if connection pool is closed
     */
    public boolean isClosed() {
        return dataSource == null || dataSource.isClosed();
    }

    /**
     * Log message with consumer name prefix
     */
    private void log(String message) {
        logger.info("[{}] {}", consumerName, message);
    }

    /**
     * Log error message with consumer name prefix
     */
    private void logErr(String message) {
        logger.error("[{}] {}", consumerName, message);
    }
}
