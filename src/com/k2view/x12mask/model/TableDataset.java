package com.k2view.x12mask.model;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Generic table dataset container used across extraction, JDBC, and trace layers.
 *
 * Structure:
 * - meta info (source file, transaction id, timestamp)
 * - tables map where key is table name and value is a list of row maps
 */
public final class TableDataset {
    private final Map<String, Object> meta = new LinkedHashMap<>();
    private final Map<String, List<Map<String, Object>>> tables = new LinkedHashMap<>();

    public TableDataset(String sourceFile, String transactionId) {
        meta.put("source_file", sourceFile);
        meta.put("transaction_id", transactionId);
        meta.put("created_at", Instant.now().toString());
    }

    public Map<String, Object> meta() { return meta; }
    public Map<String, List<Map<String, Object>>> tables() { return tables; }

    public void putTable(String tableName, List<Map<String, Object>> rows) {
        tables.put(tableName, rows);
    }
}
