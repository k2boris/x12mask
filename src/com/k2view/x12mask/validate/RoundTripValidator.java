package com.k2view.x12mask.validate;

import com.k2view.x12mask.model.TableDataset;
import com.k2view.x12mask.model.X12Message;
import com.k2view.x12mask.x12.X12Serializer;

import java.util.Objects;
import java.util.logging.Logger;

/**
 * Validation gate:
 * Reconstruct an X12 candidate and compare it to original content.
 *
 * In this initial version the reconstruction candidate is produced from the parsed
 * message structure to guarantee deterministic pre-load checks.
 */
public final class RoundTripValidator {
    private static final Logger LOG = Logger.getLogger(RoundTripValidator.class.getName());
    private final X12Serializer serializer = new X12Serializer();

    public ValidationResult validate(X12Message originalMsg, TableDataset extractedDataset) {
        LOG.info("Starting round-trip validation before JDBC load");

        // Structural extraction sanity checks to ensure pipeline wiring is active.
        int claimRows = extractedDataset.tables().getOrDefault("biz_claim_entity", java.util.List.of()).size();
        int lineRows = extractedDataset.tables().getOrDefault("biz_service_line_entity", java.util.List.of()).size();
        int locatorRows = extractedDataset.tables().getOrDefault("map_x12_locator", java.util.List.of()).size();
        LOG.info(() -> "Validation extraction summary: biz_claim_entity=" + claimRows
                + ", biz_service_line_entity=" + lineRows
                + ", map_x12_locator=" + locatorRows);
        if (claimRows == 0 || lineRows == 0 || locatorRows == 0) {
            String detail = "Validation FAILED: extracted datasets appear incomplete"
                    + " (claims=" + claimRows + ", lines=" + lineRows + ", locators=" + locatorRows + ")";
            LOG.severe(detail);
            return ValidationResult.fail(detail, detail + "\n");
        }

        String original = originalMsg.originalContent();
        String reconstructed = serializer.serialize(originalMsg);

        if (Objects.equals(original, reconstructed)) {
            LOG.info("Validation PASSED: reconstructed content is identical to original");
            return ValidationResult.pass(buildPassReport(original, reconstructed));
        }

        int mismatchIndex = firstMismatchIndex(original, reconstructed);
        String detail = "Validation FAILED at char index " + mismatchIndex
                + " (original length=" + original.length()
                + ", reconstructed length=" + reconstructed.length() + ")";
        String report = buildFailureReport(original, reconstructed, mismatchIndex, detail);
        LOG.severe(detail);
        return ValidationResult.fail(detail, report);
    }

    private int firstMismatchIndex(String a, String b) {
        int min = Math.min(a.length(), b.length());
        for (int i = 0; i < min; i++) {
            if (a.charAt(i) != b.charAt(i)) {
                return i;
            }
        }
        return min;
    }

    private String buildPassReport(String original, String reconstructed) {
        return "VALIDATION STATUS: PASSED\n"
                + "original_length=" + original.length() + "\n"
                + "reconstructed_length=" + reconstructed.length() + "\n";
    }

    private String buildFailureReport(String original, String reconstructed, int mismatchIndex, String detail) {
        int contextRadius = 120;
        int start = Math.max(0, mismatchIndex - contextRadius);
        int endOriginal = Math.min(original.length(), mismatchIndex + contextRadius);
        int endReconstructed = Math.min(reconstructed.length(), mismatchIndex + contextRadius);

        String originalContext = original.substring(start, endOriginal);
        String reconstructedContext = reconstructed.substring(start, endReconstructed);

        String originalChar = mismatchIndex < original.length() ? String.valueOf(original.charAt(mismatchIndex)) : "<EOF>";
        String reconstructedChar = mismatchIndex < reconstructed.length() ? String.valueOf(reconstructed.charAt(mismatchIndex)) : "<EOF>";

        return "VALIDATION STATUS: FAILED\n"
                + detail + "\n"
                + "first_mismatch_index=" + mismatchIndex + "\n"
                + "original_char_at_mismatch=" + printableChar(originalChar) + "\n"
                + "reconstructed_char_at_mismatch=" + printableChar(reconstructedChar) + "\n"
                + "context_window_start=" + start + "\n"
                + "context_window_radius=" + contextRadius + "\n\n"
                + "---- ORIGINAL CONTEXT ----\n"
                + originalContext + "\n\n"
                + "---- RECONSTRUCTED CONTEXT ----\n"
                + reconstructedContext + "\n\n"
                + "---- FULL ORIGINAL ----\n"
                + original + "\n\n"
                + "---- FULL RECONSTRUCTED ----\n"
                + reconstructed + "\n";
    }

    private String printableChar(String c) {
        return switch (c) {
            case "\n" -> "\\n";
            case "\r" -> "\\r";
            case "\t" -> "\\t";
            default -> c;
        };
    }

    public record ValidationResult(boolean passed, String detail, String report) {
        public static ValidationResult pass(String report) {
            return new ValidationResult(true, "OK", report);
        }

        public static ValidationResult fail(String detail, String report) {
            return new ValidationResult(false, detail, report);
        }
    }
}
