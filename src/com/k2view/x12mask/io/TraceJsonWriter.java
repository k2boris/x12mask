package com.k2view.x12mask.io;

import com.k2view.x12mask.model.TableDataset;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Writes intermediate dataset snapshots to JSON without third-party dependencies.
 *
 * The generated JSON is intentionally deterministic for easier troubleshooting.
 */
public final class TraceJsonWriter {
    private static final Logger LOG = Logger.getLogger(TraceJsonWriter.class.getName());

    public void write(Path outputPath, TableDataset dataset) throws IOException {
        Files.createDirectories(outputPath.getParent());
        String json = toJson(dataset);
        Files.writeString(outputPath, json, StandardCharsets.UTF_8);
        LOG.info(() -> "Wrote trace JSON: " + outputPath);
    }

    private String toJson(TableDataset dataset) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        sb.append("  \"meta\": ").append(mapToJson(dataset.meta(), 2)).append(",\n");
        sb.append("  \"tables\": {");

        boolean firstTable = true;
        for (Map.Entry<String, List<Map<String, Object>>> e : dataset.tables().entrySet()) {
            if (!firstTable) {
                sb.append(",");
            }
            firstTable = false;
            sb.append("\n    \"").append(escape(e.getKey())).append("\": ");
            sb.append(listOfMapsToJson(e.getValue(), 4));
        }

        if (!dataset.tables().isEmpty()) {
            sb.append("\n");
        }
        sb.append("  }\n");
        sb.append("}\n");
        return sb.toString();
    }

    private String listOfMapsToJson(List<Map<String, Object>> rows, int indent) {
        StringBuilder sb = new StringBuilder("[");
        boolean first = true;
        for (Map<String, Object> row : rows) {
            if (!first) {
                sb.append(",");
            }
            first = false;
            sb.append("\n").append(" ".repeat(indent)).append(mapToJson(row, indent));
        }
        if (!rows.isEmpty()) {
            sb.append("\n").append(" ".repeat(Math.max(indent - 2, 0)));
        }
        sb.append("]");
        return sb.toString();
    }

    private String mapToJson(Map<String, Object> map, int indent) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Object> e : map.entrySet()) {
            if (!first) {
                sb.append(",");
            }
            first = false;
            sb.append("\n").append(" ".repeat(indent));
            sb.append("\"").append(escape(e.getKey())).append("\": ").append(valueToJson(e.getValue()));
        }
        if (!map.isEmpty()) {
            sb.append("\n").append(" ".repeat(Math.max(indent - 2, 0)));
        }
        sb.append("}");
        return sb.toString();
    }

    private String valueToJson(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        }
        return "\"" + escape(String.valueOf(value)) + "\"";
    }

    private String escape(String s) {
        return s
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }
}
