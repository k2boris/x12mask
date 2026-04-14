package com.k2view.x12mask.x12;

import com.k2view.x12mask.model.X12Message;
import com.k2view.x12mask.model.X12Segment;

import java.util.StringJoiner;

/**
 * Serializes parsed X12 model back to wire format.
 */
public final class X12Serializer {
    public String serialize(X12Message message) {
        StringBuilder sb = new StringBuilder();
        for (X12Segment s : message.segments()) {
            StringJoiner joiner = new StringJoiner(String.valueOf(message.elementDelimiter()));
            for (String e : s.elements()) {
                joiner.add(e);
            }
            // Preserve original segment delimiter + trailing whitespace/newlines exactly.
            sb.append(joiner).append(s.segmentSuffix());
        }
        return sb.toString();
    }
}
