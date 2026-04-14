package com.k2view.x12mask.model;

import java.util.ArrayList;
import java.util.List;

/**
 * In-memory representation of one X12 segment.
 *
 * segmentId: segment tag (e.g., ISA, GS, NM1)
 * elements: element list split by element delimiter '*'
 * segmentSuffix: exact original bytes after segment content, including the segment
 * delimiter and any trailing whitespace/newlines before next segment.
 */
public final class X12Segment {
    private final int ordinal;
    private final String segmentId;
    private final List<String> elements;
    private final String segmentSuffix;

    public X12Segment(int ordinal, String segmentId, List<String> elements, String segmentSuffix) {
        this.ordinal = ordinal;
        this.segmentId = segmentId;
        this.elements = new ArrayList<>(elements);
        this.segmentSuffix = segmentSuffix;
    }

    public int ordinal() { return ordinal; }
    public String segmentId() { return segmentId; }
    public List<String> elements() { return elements; }
    public String segmentSuffix() { return segmentSuffix; }
}
