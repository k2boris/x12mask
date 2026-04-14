package com.k2view.x12mask.db;

import com.k2view.x12mask.model.TableDataset;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * SQLite staging persistence used before K2/Fabric masking read-back.
 */
public final class SqliteStagingService implements AutoCloseable {
    private static final Logger LOG = Logger.getLogger(SqliteStagingService.class.getName());

    private final Connection connection;
    private final Path schemaFile;

    public SqliteStagingService(String sqliteJdbcUrl, Path schemaFile) throws SQLException {
        this.connection = DriverManager.getConnection(sqliteJdbcUrl);
        this.connection.setAutoCommit(false);
        this.schemaFile = schemaFile;
        LOG.info(() -> "Connected to SQLite staging via JDBC url=" + sqliteJdbcUrl);
    }

    public void initializeSchema() throws IOException, SQLException {
        if (!Files.exists(schemaFile)) {
            throw new IOException("SQLite schema file not found: " + schemaFile.toAbsolutePath());
        }

        String sql = Files.readString(schemaFile, StandardCharsets.UTF_8);
        List<String> statements = splitSqlStatements(sql);

        try (Statement st = connection.createStatement()) {
            for (String statement : statements) {
                if (!statement.isBlank()) {
                    st.execute(statement);
                }
            }
            connection.commit();
            LOG.info(() -> "SQLite schema initialized from " + schemaFile.toAbsolutePath()
                    + " statements=" + statements.size());
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        }
    }

    public void stageExtracted(TableDataset dataset) throws SQLException {
        String transactionId = String.valueOf(dataset.meta().get("transaction_id"));
        LOG.info(() -> "Staging extracted dataset to SQLite for transaction_id=" + transactionId);

        List<String> insertOrder = new ArrayList<>(dataset.tables().keySet());
        insertOrder.remove("biz_transaction_entity");
        insertOrder.add(0, "biz_transaction_entity");

        try {
            clearTransaction(transactionId, insertOrder);
            for (String table : insertOrder) {
                List<Map<String, Object>> rows = dataset.tables().getOrDefault(table, List.of());
                if (rows.isEmpty()) {
                    continue;
                }
                insertRows(table, rows, transactionId);
            }
            connection.commit();
            LOG.info(() -> "SQLite staging commit completed for transaction_id=" + transactionId);
        } catch (SQLException e) {
            connection.rollback();
            LOG.log(Level.SEVERE, "SQLite staging failed; transaction rolled back", e);
            throw e;
        }
    }

    public int clearTransaction(String transactionId, Collection<String> tables) throws SQLException {
        int totalDeleted = 0;
        Set<String> uniqueTables = new LinkedHashSet<>(tables);

        for (String table : uniqueTables) {
            if (!isSafeIdentifier(table)) {
                LOG.warning(() -> "Skipping unsafe table identifier in SQLite clear list: " + table);
                continue;
            }

            String sql = "DELETE FROM " + table + " WHERE transaction_id = ?";
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setString(1, transactionId);
                int deleted = ps.executeUpdate();
                totalDeleted += deleted;
                final int deletedRows = deleted;
                LOG.info(() -> "SQLite CLEAR table=" + table + " deleted_rows=" + deletedRows);
            } catch (SQLException e) {
                // Keep clear robust even if some tables are absent in current schema.
                LOG.log(Level.WARNING, "SQLite CLEAR skip/failure for table=" + table, e);
            }
        }
        return totalDeleted;
    }

    public int clearTransactionAndCommit(String transactionId, Collection<String> tables) throws SQLException {
        try {
            int totalDeleted = clearTransaction(transactionId, tables);
            connection.commit();
            return totalDeleted;
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        }
    }

    private void insertRows(String table, List<Map<String, Object>> rows, String transactionId) throws SQLException {
        Map<String, Object> first = rows.get(0);
        List<String> cols = new ArrayList<>(first.keySet());
        Set<String> transactionScopedIdColumns = resolveTransactionScopedIdColumns(table);

        String columns = String.join(", ", cols);
        String placeholders = String.join(", ", Collections.nCopies(cols.size(), "?"));
        String sql = "INSERT INTO " + table + " (" + columns + ") VALUES (" + placeholders + ")";

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            int inserted = 0;
            for (Map<String, Object> row : rows) {
                for (int i = 0; i < cols.size(); i++) {
                    String col = cols.get(i);
                    Object value = row.get(col);
                    ps.setObject(i + 1, normalizeIdValue(col, value, transactionScopedIdColumns, transactionId));
                }
                ps.executeUpdate();
                inserted++;
            }
            final int insertedRows = inserted;
            LOG.info(() -> "SQLite insert table=" + table + " rows=" + insertedRows);
        }
    }

    private List<String> splitSqlStatements(String script) {
        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inSingleQuote = false;

        for (int i = 0; i < script.length(); i++) {
            char c = script.charAt(i);
            if (c == '\'') {
                inSingleQuote = !inSingleQuote;
            }
            if (c == ';' && !inSingleQuote) {
                String statement = current.toString().trim();
                if (!statement.isBlank()) {
                    statements.add(statement);
                }
                current.setLength(0);
                continue;
            }
            current.append(c);
        }

        String tail = current.toString().trim();
        if (!tail.isBlank()) {
            statements.add(tail);
        }

        return statements.stream()
                .map(String::trim)
                .filter(s -> !s.isBlank())
                .collect(Collectors.toList());
    }

    private boolean isSafeIdentifier(String value) {
        return value != null && value.matches("[A-Za-z_][A-Za-z0-9_]*");
    }

    private Set<String> resolveTransactionScopedIdColumns(String table) throws SQLException {
        Set<String> idColumns = new LinkedHashSet<>();

        // Primary-key columns.
        try (Statement st = connection.createStatement();
             ResultSet rs = st.executeQuery("PRAGMA table_info(" + table + ")")) {
            while (rs.next()) {
                int pkOrdinal = rs.getInt("pk");
                if (pkOrdinal > 0) {
                    String col = rs.getString("name");
                    if (!"transaction_id".equalsIgnoreCase(col)) {
                        idColumns.add(col);
                    }
                }
            }
        }

        // Foreign-key columns.
        try (Statement st = connection.createStatement();
             ResultSet rs = st.executeQuery("PRAGMA foreign_key_list(" + table + ")")) {
            while (rs.next()) {
                String col = rs.getString("from");
                if (col != null && !col.isBlank() && !"transaction_id".equalsIgnoreCase(col)) {
                    idColumns.add(col);
                }
            }
        }

        // Logical reference keys that are not explicit FK constraints.
        if ("map_canonical_binding".equalsIgnoreCase(table)) {
            idColumns.add("target_row_id");
        }

        return idColumns;
    }

    private Object normalizeIdValue(String col, Object value, Set<String> scopedIdColumns, String transactionId) {
        if (!(value instanceof String str)) {
            return value;
        }
        if (!scopedIdColumns.contains(col)) {
            return value;
        }
        if (str.isBlank()) {
            return value;
        }

        String prefix = transactionId + "::";
        if (str.startsWith(prefix)) {
            return str;
        }
        return prefix + str;
    }

    @Override
    public void close() {
        try {
            connection.close();
            LOG.info("SQLite staging connection closed");
        } catch (SQLException e) {
            LOG.log(Level.WARNING, "Failed to close SQLite staging connection cleanly", e);
        }
    }
}
