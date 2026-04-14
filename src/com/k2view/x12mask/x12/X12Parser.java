package com.k2view.x12mask.x12;

import com.k2view.x12mask.model.X12Message;
import com.k2view.x12mask.model.X12Segment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

/**
 * Lightweight X12 parser for delimiter-split processing.
 *
 * Important notes:
 * - This parser intentionally focuses on deterministic tokenization and tracing.
 * - Loop semantics are not fully resolved in this initial implementation.
 */
public final class X12Parser {
    private static final Logger LOG = Logger.getLogger(X12Parser.class.getName());

    public X12Message parse(String content) {
        LOG.info("Starting X12 parse");
        if (content == null || content.isBlank()) {
            throw new IllegalArgumentException("Input X12 content is empty");
        }

        char segDelim = '~';
        char elemDelim = '*';
        char compDelim = ':';

        List<X12Segment> segments = new ArrayList<>();

        // Parse with exact suffix preservation:
        // segmentContent + "~" + trailingWhitespace/newlines.
        // This allows strict round-trip byte-for-byte serialization.
        int ordinal = 0;
        int start = 0;
        while (start < content.length()) {
            int tilde = content.indexOf('~', start);
            if (tilde < 0) {
                break;
            }

            String segmentContent = content.substring(start, tilde);
            int suffixEnd = tilde + 1;
            while (suffixEnd < content.length()) {
                char c = content.charAt(suffixEnd);
                if (!Character.isWhitespace(c)) {
                    break;
                }
                suffixEnd++;
            }
            String suffix = content.substring(tilde, suffixEnd);

            if (!segmentContent.isEmpty()) {
                ordinal++;
                String[] parts = segmentContent.split("\\*", -1);
                String segmentId = parts[0];
                List<String> elements = Arrays.asList(parts);
                segments.add(new X12Segment(ordinal, segmentId, elements, suffix));
            }

            start = suffixEnd;
        }

        LOG.info(() -> "X12 parse completed; segments parsed=" + segments.size());
        return new X12Message(content, segDelim, elemDelim, compDelim, segments);
    }
}
