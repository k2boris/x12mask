package com.k2view.x12mask.io;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Logger;

/**
 * Reads X12 payload content from file.
 * Additional adapters for API/stream can be added later using the same contract.
 */
public final class X12InputReader {
    private static final Logger LOG = Logger.getLogger(X12InputReader.class.getName());

    public String readFile(Path file) throws IOException {
        LOG.info(() -> "Reading input X12 file: " + file);
        String content = Files.readString(file, StandardCharsets.UTF_8);
        LOG.info(() -> "Read completed for file " + file.getFileName() + "; character length=" + content.length());
        return content;
    }
}
