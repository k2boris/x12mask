package com.k2view.x12mask.reconstruct;

import com.k2view.x12mask.model.TableDataset;
import com.k2view.x12mask.model.X12Message;
import com.k2view.x12mask.model.X12Segment;
import com.k2view.x12mask.x12.X12Serializer;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Reconstructs masked X12 content from original structure + masked table snapshot.
 *
 * Current implementation returns structural serialization of parsed original.
 * The replacement engine can be expanded to apply locator/binding-driven substitutions.
 */
public final class MaskedReconstructor {
    private static final Logger LOG = Logger.getLogger(MaskedReconstructor.class.getName());
    private final X12Serializer serializer = new X12Serializer();

    public ReconstructionResult reconstruct(X12Message originalParsed, TableDataset maskedDataset) {
        LOG.info("Starting masked reconstruction using parsed structure and masked dataset");
        ReplacementStats stats = new ReplacementStats();

        // Create an editable copy of parsed segments to apply replacements safely.
        List<X12Segment> editableSegments = cloneSegments(originalParsed.segments());

        // Phase 1 replacement implementation:
        // Apply masked person names (biz_person_name) to NM1*IL and NM1*QC segments.
        applyMaskedPersonNames(editableSegments, maskedDataset, stats);

        X12Message reconstructedMessage = new X12Message(
                originalParsed.originalContent(),
                originalParsed.segmentDelimiter(),
                originalParsed.elementDelimiter(),
                originalParsed.componentDelimiter(),
                editableSegments
        );

        String traceReport = buildTraceReport(maskedDataset, stats);
        String result = serializer.serialize(reconstructedMessage);
        LOG.info(() -> "Masked reconstruction complete; output length=" + result.length());
        return new ReconstructionResult(result, traceReport);
    }

    private List<X12Segment> cloneSegments(List<X12Segment> source) {
        List<X12Segment> cloned = new ArrayList<>();
        for (X12Segment s : source) {
            cloned.add(new X12Segment(
                    s.ordinal(),
                    s.segmentId(),
                    new ArrayList<>(s.elements()),
                    s.segmentSuffix()
            ));
        }
        return cloned;
    }

    private void applyMaskedPersonNames(List<X12Segment> segments, TableDataset dataset, ReplacementStats stats) {
        List<Map<String, Object>> personRows = new ArrayList<>(dataset.tables().getOrDefault("biz_person_name", List.of()));
        if (personRows.isEmpty()) {
            LOG.warning("No biz_person_name rows found; no person-name masking will be applied");
            stats.notes.add("biz_person_name is empty");
            return;
        }

        // Deterministic ordering: PN-1, PN-2, ...
        personRows.sort(Comparator.comparingInt(r -> extractNumericSuffix(stringVal(r, "PERSON_NAME_ID", "person_name_id"))));

        // Target NM1 segments for subscriber/patient name masking only.
        List<X12Segment> targetNm1 = new ArrayList<>();
        for (X12Segment s : segments) {
            if (!"NM1".equals(s.segmentId())) {
                continue;
            }
            String entityCode = safeElement(s.elements(), 1); // NM101
            if ("IL".equals(entityCode) || "QC".equals(entityCode)) {
                targetNm1.add(s);
            }
        }

        stats.personNameRows = personRows.size();
        stats.nm1Targets = targetNm1.size();

        int applied = 0;
        int pairCount = Math.min(personRows.size(), targetNm1.size());
        for (int i = 0; i < pairCount; i++) {
            Map<String, Object> row = personRows.get(i);
            X12Segment seg = targetNm1.get(i);

            String maskedFirst = stringVal(row, "FIRST_NAME", "first_name");
            String maskedLast = stringVal(row, "LAST_NAME", "last_name");
            String maskedMiddle = stringVal(row, "MIDDLE_NAME", "middle_name");

            // NM1 element positions in this parser model:
            // index 3 = NM103 (last), index 4 = NM104 (first), index 5 = NM105 (middle)
            setElement(seg.elements(), 3, maskedLast);
            setElement(seg.elements(), 4, maskedFirst);
            setElement(seg.elements(), 5, maskedMiddle);

            applied++;
            stats.replacementsApplied++;
            if (stats.samples.size() < 20) {
                stats.samples.add("NM1 ordinal=" + seg.ordinal()
                        + " <- " + stringVal(row, "PERSON_NAME_ID", "person_name_id")
                        + " [last=" + maskedLast + ", first=" + maskedFirst + ", middle=" + maskedMiddle + "]");
            }
        }

        if (personRows.size() != targetNm1.size()) {
            stats.notes.add("biz_person_name rows (" + personRows.size()
                    + ") and NM1 IL/QC targets (" + targetNm1.size() + ") differ");
        }
        final int appliedCount = applied;
        final int personRowsCount = personRows.size();
        final int nm1TargetCount = targetNm1.size();
        LOG.info(() -> "Applied masked person-name replacements=" + appliedCount
                + ", personRows=" + personRowsCount + ", nm1Targets=" + nm1TargetCount);
    }

    private String buildTraceReport(TableDataset maskedDataset, ReplacementStats stats) {
        StringBuilder sb = new StringBuilder();
        sb.append("RECONSTRUCTION TRACE REPORT\n");
        sb.append("status=REPLACEMENT_PHASE_1\n");
        sb.append("note=Phase 1 applies biz_person_name to NM1 IL/QC segments\n\n");

        Map<String, List<Map<String, Object>>> tables = maskedDataset.tables();
        List<Map<String, Object>> bindings = tables.getOrDefault("map_canonical_binding", List.of());
        List<Map<String, Object>> locators = tables.getOrDefault("map_x12_locator", List.of());

        Set<String> bindingTargets = new LinkedHashSet<>();
        for (Map<String, Object> b : bindings) {
            Object t = b.get("TARGET_TABLE");
            if (t != null) {
                bindingTargets.add(String.valueOf(t));
            }
        }

        Set<String> maskedBizTables = new LinkedHashSet<>();
        List<String> maskedSamples = new ArrayList<>();
        int totalMaskedCells = 0;
        for (Map.Entry<String, List<Map<String, Object>>> e : tables.entrySet()) {
            String table = e.getKey();
            if (!table.startsWith("biz_")) {
                continue;
            }
            for (Map<String, Object> row : e.getValue()) {
                for (Map.Entry<String, Object> col : row.entrySet()) {
                    Object v = col.getValue();
                    if (v instanceof String sv && sv.contains("MASK")) {
                        totalMaskedCells++;
                        maskedBizTables.add(table);
                        if (maskedSamples.size() < 25) {
                            maskedSamples.add(table + "." + col.getKey() + "=" + sv);
                        }
                    }
                }
            }
        }

        Set<String> maskedBizAsBindingNames = new LinkedHashSet<>();
        for (String t : maskedBizTables) {
            maskedBizAsBindingNames.add(t);
        }
        Set<String> coveredMaskedTargets = new LinkedHashSet<>(maskedBizAsBindingNames);
        coveredMaskedTargets.retainAll(bindingTargets);

        sb.append("tables.total=").append(tables.size()).append("\n");
        sb.append("map_x12_locator.rows=").append(locators.size()).append("\n");
        sb.append("map_canonical_binding.rows=").append(bindings.size()).append("\n");
        sb.append("map_canonical_binding.target_tables=").append(bindingTargets).append("\n");
        sb.append("masked_cells.total=").append(totalMaskedCells).append("\n");
        sb.append("masked_tables.biz=").append(maskedBizTables).append("\n");
        sb.append("masked_tables.as_binding_names=").append(maskedBizAsBindingNames).append("\n");
        sb.append("masked_binding_intersection=").append(coveredMaskedTargets).append("\n");
        sb.append("replacements_applied=").append(stats.replacementsApplied).append("\n");
        sb.append("phase1.person_name_rows=").append(stats.personNameRows).append("\n");
        sb.append("phase1.nm1_target_segments=").append(stats.nm1Targets).append("\n\n");

        sb.append("masked_samples (up to 25):\n");
        for (String sample : maskedSamples) {
            sb.append("- ").append(sample).append("\n");
        }

        sb.append("\nreplacement_samples (up to 20):\n");
        for (String sample : stats.samples) {
            sb.append("- ").append(sample).append("\n");
        }

        if (!stats.notes.isEmpty()) {
            sb.append("\nnotes:\n");
            for (String note : stats.notes) {
                sb.append("- ").append(note).append("\n");
            }
        }

        return sb.toString();
    }

    private String stringVal(Map<String, Object> row, String upper, String lower) {
        Object v = row.containsKey(upper) ? row.get(upper) : row.get(lower);
        return v == null ? "" : String.valueOf(v);
    }

    private int extractNumericSuffix(String id) {
        if (id == null) return Integer.MAX_VALUE;
        int dash = id.lastIndexOf('-');
        if (dash < 0 || dash + 1 >= id.length()) return Integer.MAX_VALUE;
        try {
            return Integer.parseInt(id.substring(dash + 1));
        } catch (NumberFormatException ex) {
            return Integer.MAX_VALUE;
        }
    }

    private String safeElement(List<String> elements, int index) {
        if (index < 0 || index >= elements.size()) {
            return "";
        }
        return elements.get(index);
    }

    private void setElement(List<String> elements, int index, String value) {
        while (elements.size() <= index) {
            elements.add("");
        }
        elements.set(index, value == null ? "" : value);
    }

    private static final class ReplacementStats {
        int replacementsApplied = 0;
        int personNameRows = 0;
        int nm1Targets = 0;
        List<String> samples = new ArrayList<>();
        List<String> notes = new ArrayList<>();
    }

    public record ReconstructionResult(String maskedX12, String traceReport) {
    }
}
