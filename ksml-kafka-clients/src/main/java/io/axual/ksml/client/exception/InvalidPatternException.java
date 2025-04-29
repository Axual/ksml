package io.axual.ksml.client.exception;

import lombok.Getter;

/**
 * Exception indicating an invalid pattern was provided. Formats the exception message to contain
 * the provided pattern and reason.
 */
@Getter
public class InvalidPatternException extends RuntimeException {
    private final String pattern;
    private final String reason;

    /**
     * Construct the exception by providing the offending pattern and reason for failure.
     * Formats the exception message to contain both the provided pattern and reason.
     *
     * @param pattern The pattern causing the exception
     * @param reason  The reason why the pattern is invalid
     */
    public InvalidPatternException(String pattern, String reason) {
        super(formatMessage(pattern, reason));
        this.pattern = pattern;
        this.reason = reason;
    }

    private static String formatMessage(String pattern, String reason) {
        return """
                Invalid Pattern: %s
                Reason: %s
                """.formatted(pattern, reason);
    }
}
