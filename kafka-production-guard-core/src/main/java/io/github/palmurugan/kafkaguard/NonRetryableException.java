package io.github.palmurugan.kafkaguard;

/**
 * Extend this to tell the guard "retrying will never help": goes straight to the DLT.
 */
public class NonRetryableException extends RuntimeException {
    protected NonRetryableException(String message) {
        super(message);
    }

    protected NonRetryableException(String message, Throwable cause) {
        super(message, cause);
    }
}
