package com.k2view.x12mask;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Centralized immutable configuration holder.
 *
 * Design intent:
 * - Keep all runtime behavior configurable from one top-level properties file.
 * - Avoid hardcoded paths and connection details in code.
 * - Make operational toggles (log level, fail-fast) easy to adjust without rebuild.
 */
public final class AppConfig {
    public static final String PIPELINE_MODE_DIRECT_JDBC = "direct_jdbc";
    public static final String PIPELINE_MODE_SQLITE_STAGING = "sqlite_staging";

    private final Path inputDir;
    private final Path outputDir;
    private final Path traceDir;
    private final Path connectorDir;
    private final String logLevel;
    private final String pipelineMode;
    private final int transactionStart;
    private final String jdbcUrl;
    private final String jdbcSessionInitTemplate;
    private final String sqliteJdbcUrl;
    private final Path sqliteSchemaFile;
    private final String filePattern;
    private final boolean batchFailFast;
    private final List<String> readTables;
    private final List<String> clearTables;
    private final List<String> sqliteClearTables;

    private AppConfig(
            Path inputDir,
            Path outputDir,
            Path traceDir,
            Path connectorDir,
            String logLevel,
            String pipelineMode,
            int transactionStart,
            String jdbcUrl,
            String jdbcSessionInitTemplate,
            String sqliteJdbcUrl,
            Path sqliteSchemaFile,
            String filePattern,
            boolean batchFailFast,
            List<String> readTables,
            List<String> clearTables,
            List<String> sqliteClearTables) {
        this.inputDir = inputDir;
        this.outputDir = outputDir;
        this.traceDir = traceDir;
        this.connectorDir = connectorDir;
        this.logLevel = logLevel;
        this.pipelineMode = pipelineMode;
        this.transactionStart = transactionStart;
        this.jdbcUrl = jdbcUrl;
        this.jdbcSessionInitTemplate = jdbcSessionInitTemplate;
        this.sqliteJdbcUrl = sqliteJdbcUrl;
        this.sqliteSchemaFile = sqliteSchemaFile;
        this.filePattern = filePattern;
        this.batchFailFast = batchFailFast;
        this.readTables = readTables;
        this.clearTables = clearTables;
        this.sqliteClearTables = sqliteClearTables;
    }

    /**
     * Load configuration from a .properties file path.
     */
    public static AppConfig load(Path propertiesPath) throws IOException {
        Properties p = new Properties();
        try (InputStream in = Files.newInputStream(propertiesPath)) {
            p.load(in);
        }

        String readTablesRaw = p.getProperty("app.read.tables", "");
        List<String> readTables = Arrays.stream(readTablesRaw.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());

        String clearTablesRaw = p.getProperty("app.clear.tables", "");
        List<String> clearTables = Arrays.stream(clearTablesRaw.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());

        String sqliteClearTablesRaw = p.getProperty("app.sqlite.clear.tables", clearTablesRaw);
        List<String> sqliteClearTables = Arrays.stream(sqliteClearTablesRaw.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());

        return new AppConfig(
                Path.of(p.getProperty("app.input.dir", "data/in")),
                Path.of(p.getProperty("app.output.dir", "data/out")),
                Path.of(p.getProperty("app.trace.dir", "data/trace")),
                Path.of(p.getProperty("app.connector.dir", "connector")),
                p.getProperty("app.log.level", "INFO"),
                p.getProperty("app.pipeline.mode", PIPELINE_MODE_DIRECT_JDBC),
                Integer.parseInt(p.getProperty("app.transaction.start", "1002")),
                p.getProperty("app.jdbc.url", ""),
                p.getProperty("app.jdbc.session.init.template", "GET X12Map_02.'%s'"),
                p.getProperty("app.sqlite.jdbc.url", "jdbc:sqlite:connector/x12-staging.db"),
                Path.of(p.getProperty("app.sqlite.schema.file", "connector/schema.sql")),
                p.getProperty("app.file.pattern", ".*\\.x12$"),
                Boolean.parseBoolean(p.getProperty("app.batch.fail.fast", "false")),
                readTables,
                clearTables,
                sqliteClearTables
        );
    }

    public Path inputDir() { return inputDir; }
    public Path outputDir() { return outputDir; }
    public Path traceDir() { return traceDir; }
    public Path connectorDir() { return connectorDir; }
    public String logLevel() { return logLevel; }
    public String pipelineMode() { return pipelineMode; }
    public int transactionStart() { return transactionStart; }
    public String jdbcUrl() { return jdbcUrl; }
    public String jdbcSessionInitTemplate() { return jdbcSessionInitTemplate; }
    public String sqliteJdbcUrl() { return sqliteJdbcUrl; }
    public Path sqliteSchemaFile() { return sqliteSchemaFile; }
    public String filePattern() { return filePattern; }
    public boolean batchFailFast() { return batchFailFast; }
    public List<String> readTables() { return readTables; }
    public List<String> clearTables() { return clearTables; }
    public List<String> sqliteClearTables() { return sqliteClearTables; }

    public boolean isSqliteStagingMode() {
        return PIPELINE_MODE_SQLITE_STAGING.equalsIgnoreCase(pipelineMode);
    }
}
