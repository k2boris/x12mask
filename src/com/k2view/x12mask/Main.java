package com.k2view.x12mask;

import com.k2view.x12mask.db.SqliteStagingService;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Logger;

/**
 * Entry point for the X12 masking application.
 *
 * Usage:
 *   java com.k2view.x12mask.Main stage [application.properties]
 *   java com.k2view.x12mask.Main mask <transaction_id> [application.properties]
 *   java com.k2view.x12mask.Main clear <transaction_id> [application.properties]
 */
public final class Main {
    private static final Logger LOG = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && "clear".equalsIgnoreCase(args[0])) {
            runClearMode(args);
            return;
        }
        if (args.length > 0 && "mask".equalsIgnoreCase(args[0])) {
            runMaskMode(args);
            return;
        }
        if (args.length > 0 && "stage".equalsIgnoreCase(args[0])) {
            runStageMode(args);
            return;
        }
        if (args.length > 0) {
            throw new IllegalArgumentException("Usage: Main stage [application.properties] | mask <transaction_id> [application.properties] | clear <transaction_id> [application.properties]");
        }

        // Backward compatibility: no args defaults to stage mode.
        Path configPath = Path.of("application.properties");
        AppConfig config = AppConfig.load(configPath);

        LoggingConfigurator.configure(config.logLevel());
        LOG.info(() -> "Configuration loaded from " + configPath.toAbsolutePath());

        PipelineOrchestrator orchestrator = new PipelineOrchestrator(config);
        orchestrator.runStageBatch();
    }

    private static void runStageMode(String[] args) throws Exception {
        Path configPath = args.length > 1 ? Path.of(args[1]) : Path.of("application.properties");
        AppConfig config = AppConfig.load(configPath);
        LoggingConfigurator.configure(config.logLevel());
        Files.createDirectories(config.connectorDir());

        LOG.info(() -> "Running STAGE mode using config=" + configPath.toAbsolutePath());
        PipelineOrchestrator orchestrator = new PipelineOrchestrator(config);
        orchestrator.runStageBatch();
    }

    private static void runMaskMode(String[] args) throws Exception {
        if (args.length < 2) {
            throw new IllegalArgumentException("Usage: Main mask <transaction_id> [application.properties]");
        }

        String transactionId = args[1];
        Path configPath = args.length > 2 ? Path.of(args[2]) : Path.of("application.properties");
        AppConfig config = AppConfig.load(configPath);
        LoggingConfigurator.configure(config.logLevel());

        LOG.info(() -> "Running MASK mode for transaction_id=" + transactionId
                + " using config=" + configPath.toAbsolutePath());
        PipelineOrchestrator orchestrator = new PipelineOrchestrator(config);
        orchestrator.runMaskForTransaction(transactionId);
    }

    private static void runClearMode(String[] args) throws Exception {
        if (args.length < 2) {
            throw new IllegalArgumentException("Usage: Main clear <transaction_id> [application.properties]");
        }

        String transactionId = args[1];
        Path configPath = args.length > 2 ? Path.of(args[2]) : Path.of("application.properties");
        AppConfig config = AppConfig.load(configPath);
        LoggingConfigurator.configure(config.logLevel());
        Files.createDirectories(config.connectorDir());

        LOG.warning(() -> "Running CLEAR mode for transaction_id=" + transactionId
                + " using config=" + configPath.toAbsolutePath());

        try (SqliteStagingService sqlite = new SqliteStagingService(config.sqliteJdbcUrl(), config.sqliteSchemaFile())) {
            sqlite.initializeSchema();
            int deleted = sqlite.clearTransactionAndCommit(transactionId, config.sqliteClearTables());
            final int deletedRows = deleted;
            LOG.warning(() -> "CLEAR mode completed (SQLite staging only); total rows deleted=" + deletedRows);
        }
    }
}
