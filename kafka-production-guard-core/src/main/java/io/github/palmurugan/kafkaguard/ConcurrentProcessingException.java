package io.github.palmurugan.kafkaguard;

/**
 * Thrown when a record is already being processed elsewhere. Retryable by design.
 */
public class ConcurrentProcessingException extends RuntimeException {
    public ConcurrentProcessingException(String key) {
        super("Record is already being processed: " + key);
    }
}
