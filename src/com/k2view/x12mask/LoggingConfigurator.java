package com.k2view.x12mask;

import java.util.Locale;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * Logging bootstrap utility.
 *
 * Goals:
 * - Expose simple, centralized level control from configuration.
 * - Ensure all component loggers follow the same formatting and level.
 * - Provide highly readable logs for operational troubleshooting.
 */
public final class LoggingConfigurator {
    private LoggingConfigurator() {
    }

    public static void configure(String configuredLevel) {
        Level level = toLevel(configuredLevel);

        // Reset root logger handlers to enforce consistent output format.
        Logger root = Logger.getLogger("");
        for (Handler h : root.getHandlers()) {
            root.removeHandler(h);
        }

        ConsoleHandler console = new ConsoleHandler();
        console.setLevel(level);
        console.setFormatter(new SingleLineFormatter());

        root.setLevel(level);
        root.addHandler(console);
    }

    private static Level toLevel(String value) {
        if (value == null) {
            return Level.INFO;
        }
        String n = value.trim().toUpperCase(Locale.ROOT);
        return switch (n) {
            case "ERROR", "SEVERE" -> Level.SEVERE;
            case "WARN", "WARNING" -> Level.WARNING;
            case "DEBUG", "FINE" -> Level.FINE;
            case "INFO" -> Level.INFO;
            default -> Level.INFO;
        };
    }

    /**
     * Compact log format: timestamp level logger - message
     */
    private static final class SingleLineFormatter extends Formatter {
        @Override
        public String format(LogRecord record) {
            String base = String.format(
                    "%1$tF %1$tT [%2$-7s] %3$s - %4$s%n",
                    record.getMillis(),
                    record.getLevel().getName(),
                    record.getLoggerName(),
                    formatMessage(record)
            );
            if (record.getThrown() == null) {
                return base;
            }

            // Include full stack trace when an exception is present.
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            record.getThrown().printStackTrace(pw);
            pw.flush();
            return base + sw;
        }
    }
}
