package com.k2view.x12mask.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Parsed X12 message model.
 */
public final class X12Message {
    private final String originalContent;
    private final char segmentDelimiter;
    private final char elementDelimiter;
    private final char componentDelimiter;
    private final List<X12Segment> segments;

    public X12Message(String originalContent,
                      char segmentDelimiter,
                      char elementDelimiter,
                      char componentDelimiter,
                      List<X12Segment> segments) {
        this.originalContent = originalContent;
        this.segmentDelimiter = segmentDelimiter;
        this.elementDelimiter = elementDelimiter;
        this.componentDelimiter = componentDelimiter;
        this.segments = new ArrayList<>(segments);
    }

    public String originalContent() { return originalContent; }
    public char segmentDelimiter() { return segmentDelimiter; }
    public char elementDelimiter() { return elementDelimiter; }
    public char componentDelimiter() { return componentDelimiter; }
    public List<X12Segment> segments() { return segments; }
}
