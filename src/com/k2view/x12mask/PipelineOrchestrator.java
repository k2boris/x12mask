package com.k2view.x12mask;

import com.k2view.x12mask.db.FabricJdbcService;
import com.k2view.x12mask.db.SqliteStagingService;
import com.k2view.x12mask.extract.ExtractionService;
import com.k2view.x12mask.io.TraceJsonWriter;
import com.k2view.x12mask.io.X12InputReader;
import com.k2view.x12mask.model.TableDataset;
import com.k2view.x12mask.model.X12Message;
import com.k2view.x12mask.reconstruct.MaskedReconstructor;
import com.k2view.x12mask.validate.RoundTripValidator;
import com.k2view.x12mask.x12.X12Parser;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * Orchestrates staged execution:
 * - Stage 1: parse/extract/validate/trace + SQLite staging
 * - Stage 2: JDBC read masked + reconstruct masked X12
 *
 * Stage 1 per file:
 * 1) read
 * 2) parse
 * 3) extract
 * 4) write extracted trace
 * 5) validate round-trip
 * 6) stage extracted data (SQLite or direct JDBC, by mode)
 * 7) persist transaction context for Stage 2
 */
public final class PipelineOrchestrator {
    private static final Logger LOG = Logger.getLogger(PipelineOrchestrator.class.getName());

    private final AppConfig config;
    private final X12InputReader inputReader = new X12InputReader();
    private final X12Parser parser = new X12Parser();
    private final ExtractionService extractor = new ExtractionService();
    private final TraceJsonWriter traceWriter = new TraceJsonWriter();
    private final RoundTripValidator validator = new RoundTripValidator();
    private final MaskedReconstructor reconstructor = new MaskedReconstructor();

    public PipelineOrchestrator(AppConfig config) {
        this.config = config;
    }

    public void runStageBatch() throws IOException {
        Files.createDirectories(config.inputDir());
        Files.createDirectories(config.outputDir());
        Files.createDirectories(config.traceDir());
        Files.createDirectories(config.connectorDir());

        Pattern pattern = Pattern.compile(config.filePattern());
        List<Path> files;
        try (var stream = Files.list(config.inputDir())) {
            files = stream
                    .filter(Files::isRegularFile)
                    .filter(p -> pattern.matcher(p.getFileName().toString()).matches())
                    .sorted()
                    .toList();
        }

        LOG.info(() -> "Batch start: input directory=" + config.inputDir() + ", matching files=" + files.size());

        int transactionSequence = config.transactionStart();
        for (Path file : files) {
            try {
                stageOneFile(file, "TXN-" + transactionSequence);
                transactionSequence++;
            } catch (Exception e) {
                LOG.log(Level.SEVERE, "File processing failed for " + file.getFileName(), e);
                if (config.batchFailFast()) {
                    throw new RuntimeException("Fail-fast enabled; stopping batch", e);
                }
            }
        }

        LOG.info("Batch completed");
    }

    /**
     * Backward compatible alias for older call sites.
     */
    public void runBatch() throws IOException {
        runStageBatch();
    }

    public void runMaskForTransaction(String transactionId) throws Exception {
        Files.createDirectories(config.outputDir());
        Files.createDirectories(config.traceDir());

        TransactionContext ctx = readTransactionContext(transactionId);
        LOG.info(() -> "Stage 2 mask start for transaction_id=" + transactionId + ", source=" + ctx.sourceFile());

        String content = Files.readString(ctx.sourceSnapshot(), StandardCharsets.UTF_8);
        X12Message parsed = parser.parse(content);

        TableDataset maskedRead;
        try (FabricJdbcService jdbc = new FabricJdbcService(config.jdbcUrl())) {
            jdbc.initializeSession(config.jdbcSessionInitTemplate(), transactionId);
            maskedRead = jdbc.readMaskedDataset(ctx.sourceFile(), transactionId, config.readTables());
        }

        Path maskedTrace = config.traceDir().resolve(ctx.rootName() + ".masked.json");
        traceWriter.write(maskedTrace, maskedRead);

        MaskedReconstructor.ReconstructionResult reconstructionResult = reconstructor.reconstruct(parsed, maskedRead);
        Path reconTrace = config.traceDir().resolve(ctx.rootName() + ".reconstruct.trace.txt");
        Files.writeString(reconTrace, reconstructionResult.traceReport(), StandardCharsets.UTF_8);
        LOG.info(() -> "Wrote reconstruction trace: " + reconTrace.toAbsolutePath());

        String maskedX12 = reconstructionResult.maskedX12();
        Path outPath = config.outputDir().resolve(ctx.rootName() + ".masked.x12");
        Files.writeString(outPath, maskedX12, StandardCharsets.UTF_8);

        LOG.info(() -> "Stage 2 mask completed for transaction_id=" + transactionId + "; output=" + outPath);
    }

    private void stageOneFile(Path file, String transactionId) throws Exception {
        String sourceName = file.getFileName().toString();
        String rootName = sourceName.endsWith(".x12") ? sourceName.substring(0, sourceName.length() - 4) : sourceName;

        LOG.info(() -> "Stage 1 processing file=" + sourceName + ", transaction_id=" + transactionId);

        String content = inputReader.readFile(file);
        X12Message parsed = parser.parse(content);

        TableDataset extracted = extractor.extract(sourceName, transactionId, parsed);
        Path extractedTrace = config.traceDir().resolve(rootName + ".extracted.json");
        traceWriter.write(extractedTrace, extracted);

        RoundTripValidator.ValidationResult validationResult = validator.validate(parsed, extracted);
        if (!validationResult.passed()) {
            Path diffPath = config.outputDir().resolve(rootName + ".validation.diff.txt");
            Files.writeString(diffPath, validationResult.report(), StandardCharsets.UTF_8);
            LOG.severe(() -> "Validation diff written to " + diffPath.toAbsolutePath());
            throw new IllegalStateException("Validation failed: " + validationResult.detail());
        }

        if (config.isSqliteStagingMode()) {
            try (SqliteStagingService sqlite = new SqliteStagingService(config.sqliteJdbcUrl(), config.sqliteSchemaFile())) {
                sqlite.initializeSchema();
                sqlite.stageExtracted(extracted);
            }
        } else {
            try (FabricJdbcService jdbc = new FabricJdbcService(config.jdbcUrl())) {
                jdbc.initializeSession(config.jdbcSessionInitTemplate(), transactionId);
                jdbc.insertExtracted(extracted);
            }
        }

        Path sourceSnapshot = config.traceDir().resolve(transactionId + ".source.x12");
        Files.writeString(sourceSnapshot, content, StandardCharsets.UTF_8);
        writeTransactionContext(transactionId, sourceName, rootName, sourceSnapshot);

        LOG.info(() -> "Stage 1 completed for file=" + sourceName + ", transaction_id=" + transactionId);
    }

    private void writeTransactionContext(
            String transactionId,
            String sourceFile,
            String rootName,
            Path sourceSnapshot) throws IOException {
        Properties properties = new Properties();
        properties.setProperty("transaction_id", transactionId);
        properties.setProperty("source_file", sourceFile);
        properties.setProperty("root_name", rootName);
        properties.setProperty("source_snapshot", sourceSnapshot.toString());

        Path contextFile = contextFileFor(transactionId);
        try (var out = Files.newOutputStream(contextFile)) {
            properties.store(out, "Transaction context for stage-2 reconstruction");
        }
    }

    private TransactionContext readTransactionContext(String transactionId) throws IOException {
        Path contextFile = contextFileFor(transactionId);
        if (!Files.exists(contextFile)) {
            throw new IOException("Transaction context file not found for " + transactionId + ": " + contextFile.toAbsolutePath());
        }

        Properties properties = new Properties();
        try (var in = Files.newInputStream(contextFile)) {
            properties.load(in);
        }

        String sourceFile = properties.getProperty("source_file", "");
        String rootName = properties.getProperty("root_name", "");
        Path sourceSnapshot = Path.of(properties.getProperty("source_snapshot", ""));
        if (sourceFile.isBlank() || rootName.isBlank() || sourceSnapshot.toString().isBlank()) {
            throw new IOException("Invalid transaction context for " + transactionId + ": " + contextFile.toAbsolutePath());
        }
        if (!Files.exists(sourceSnapshot)) {
            throw new IOException("Source snapshot not found for " + transactionId + ": " + sourceSnapshot.toAbsolutePath());
        }

        return new TransactionContext(sourceFile, rootName, sourceSnapshot);
    }

    private Path contextFileFor(String transactionId) {
        return config.traceDir().resolve(transactionId + ".context.properties");
    }

    private record TransactionContext(String sourceFile, String rootName, Path sourceSnapshot) {
    }
}
