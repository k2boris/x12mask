package com.k2view.x12mask.db;

import com.k2view.x12mask.model.TableDataset;

import java.sql.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * JDBC integration layer for K2View Fabric.
 *
 * Responsibilities:
 * - Open one JDBC connection for the pipeline run.
 * - Initialize LU/session context for the transaction.
 * - Insert extracted biz_ and map_ rows.
 * - Read masked biz_ and map_ rows back.
 */
public final class FabricJdbcService implements AutoCloseable {
    private static final Logger LOG = Logger.getLogger(FabricJdbcService.class.getName());

    private final Connection connection;

    public FabricJdbcService(String jdbcUrl) throws SQLException {
        this.connection = DriverManager.getConnection(jdbcUrl);
        this.connection.setAutoCommit(false);
        LOG.info("Connected to Fabric via JDBC");
    }

    public void initializeSession(String initTemplate, String transactionId) throws SQLException {
        String cmd = String.format(initTemplate, transactionId);
        LOG.info(() -> "Initializing Fabric session context using command: " + cmd);
        try (Statement st = connection.createStatement()) {
            st.execute(cmd);
        }
    }

    public void insertExtracted(TableDataset dataset) throws SQLException {
        LOG.info("Starting JDBC insert for extracted dataset");

        // Stable order: transaction root first, then all remaining tables in map iteration order.
        List<String> order = new ArrayList<>(dataset.tables().keySet());
        order.remove("biz_transaction_entity");
        order.add(0, "biz_transaction_entity");

        // Transaction boundaries are explicit here (autoCommit=false is configured in constructor).
        LOG.info("JDBC transaction BEGIN (insertExtracted)");
        try {
            for (String table : order) {
                List<Map<String, Object>> rows = dataset.tables().getOrDefault(table, List.of());
                if (rows.isEmpty()) {
                    LOG.fine(() -> "Skipping empty table: " + table);
                    continue;
                }
                insertRows(table, rows);
            }

            connection.commit();
            LOG.info("JDBC transaction COMMIT (insertExtracted)");
        } catch (SQLException e) {
            LOG.log(Level.SEVERE, "JDBC insert failed; transaction will be rolled back", e);
            try {
                connection.rollback();
                LOG.warning("JDBC transaction ROLLBACK completed (insertExtracted)");
            } catch (SQLException rollbackError) {
                LOG.log(Level.SEVERE, "JDBC transaction ROLLBACK failed", rollbackError);
            }
            throw e;
        }
    }

    public TableDataset readMaskedDataset(String sourceFileName, String transactionId, List<String> readTables) throws SQLException {
        LOG.info("Starting JDBC read-back for masked dataset");
        TableDataset dataset = new TableDataset(sourceFileName, transactionId);

        for (String table : readTables) {
            List<Map<String, Object>> rows = selectRows(table, transactionId);
            dataset.putTable(table, rows);
            LOG.info(() -> "Read table " + table + "; rows=" + rows.size());
        }
        return dataset;
    }

    /**
     * Clear all rows for a specific transaction id from configured tables.
     *
     * Important behavior:
     * - Expects table list to be ordered child->parent to avoid FK issues.
     * - Skips non-existing tables or tables without transaction_id filter capability.
     * - Commits at end when all deletes are attempted.
     */
    public int clearTransaction(String transactionId, List<String> tables) throws SQLException {
        LOG.warning(() -> "Starting CLEAR operation for transaction_id=" + transactionId + "; table count=" + tables.size());
        int totalDeleted = 0;

        for (String table : tables) {
            if (!isSafeIdentifier(table)) {
                LOG.warning(() -> "Skipping unsafe table identifier in clear list: " + table);
                continue;
            }

            String sql = "DELETE FROM " + table + " WHERE transaction_id = ?";
            LOG.fine(() -> "DEBUG SQL (clear): " + sql + " ; params=[transaction_id=" + transactionId + "]");
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setString(1, transactionId);
                int deleted = ps.executeUpdate();
                totalDeleted += deleted;
                final int deletedCount = deleted;
                LOG.info(() -> "CLEAR table=" + table + " deleted_rows=" + deletedCount);
            } catch (SQLException e) {
                LOG.log(Level.WARNING, "CLEAR skip/failure for table=" + table + " using transaction_id filter", e);
            }
        }

        connection.commit();
        final int committedDeleteCount = totalDeleted;
        LOG.warning(() -> "CLEAR operation completed for transaction_id=" + transactionId
                + "; total_deleted_rows=" + committedDeleteCount);
        return totalDeleted;
    }

    private void insertRows(String table, List<Map<String, Object>> rows) throws SQLException {
        Map<String, Object> first = rows.get(0);
        List<String> cols = new ArrayList<>(first.keySet());

        String columns = String.join(", ", cols);
        String placeholders = String.join(", ", Collections.nCopies(cols.size(), "?"));
        String sql = "INSERT INTO " + table + " (" + columns + ") VALUES (" + placeholders + ")";

        LOG.fine(() -> "Prepared insert SQL for table " + table + ": " + sql);

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            int insertedCount = 0;
            for (Map<String, Object> row : rows) {
                for (int i = 0; i < cols.size(); i++) {
                    ps.setObject(i + 1, row.get(cols.get(i)));
                }
                ps.executeUpdate();
                insertedCount++;
            }
            final int count = insertedCount;
            LOG.info(() -> "Inserted rows into " + table + ": " + count);
        } catch (SQLException e) {
            String diagnostic = "JDBC insert failure"
                    + " | table=" + table
                    + " | sql=" + sql
                    + " | row_count=" + rows.size()
                    + " | columns=" + cols
                    + " | sql_state=" + e.getSQLState()
                    + " | vendor_code=" + e.getErrorCode()
                    + " | message=" + e.getMessage();
            LOG.severe(diagnostic);

            // Print a compact first-row snapshot to make bad value diagnosis easier.
            Map<String, Object> firstRow = rows.isEmpty() ? Map.of() : rows.get(0);
            LOG.severe(() -> "Insert first-row sample for table " + table + ": " + compactRow(firstRow));

            // Walk chained SQL exceptions (common in JDBC drivers).
            SQLException next = e.getNextException();
            int depth = 1;
            while (next != null) {
                final int d = depth;
                final SQLException ex = next;
                LOG.severe(() -> "SQL chained exception #" + d
                        + " | sql_state=" + ex.getSQLState()
                        + " | vendor_code=" + ex.getErrorCode()
                        + " | message=" + ex.getMessage());
                next = next.getNextException();
                depth++;
            }
            throw e;
        }
    }

    private List<Map<String, Object>> selectRows(String table, String transactionId) throws SQLException {
        // In Fabric, transaction context is already established via:
        // GET X12Map_02.'<transaction_id>'
        // Therefore reads should be unfiltered table selects under current LU context.
        String fullSql = "SELECT * FROM " + table;
        LOG.fine(() -> "DEBUG SQL (context-scoped): " + fullSql + " ; lu_transaction_id=" + transactionId);

        try (Statement st = connection.createStatement(); ResultSet rs = st.executeQuery(fullSql)) {
            return readResultSet(rs);
        }
    }

    private List<Map<String, Object>> readResultSet(ResultSet rs) throws SQLException {
        List<Map<String, Object>> rows = new ArrayList<>();
        ResultSetMetaData md = rs.getMetaData();
        int colCount = md.getColumnCount();

        while (rs.next()) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (int i = 1; i <= colCount; i++) {
                row.put(md.getColumnName(i), rs.getObject(i));
            }
            rows.add(row);
        }
        return rows;
    }

    private boolean isSafeIdentifier(String value) {
        return value != null && value.matches("[A-Za-z_][A-Za-z0-9_]*");
    }

    private String compactRow(Map<String, Object> row) {
        return row.entrySet().stream()
                .map(e -> e.getKey() + "=" + String.valueOf(e.getValue()))
                .collect(Collectors.joining(", ", "{", "}"));
    }

    @Override
    public void close() {
        try {
            connection.close();
            LOG.info("JDBC connection closed");
        } catch (SQLException e) {
            LOG.log(Level.WARNING, "Failed to close JDBC connection cleanly", e);
        }
    }
}
