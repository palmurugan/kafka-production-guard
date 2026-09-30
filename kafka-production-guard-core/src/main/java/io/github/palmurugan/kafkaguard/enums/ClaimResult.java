package io.github.palmurugan.kafkaguard.enums;

/**
 * Outcome of trying to claim a message for processing.
 */
public enum ClaimResult {
    /**
     * This caller now owns the message and should process it.
     */
    ACQUIRED,
    /**
     * Someone else is processing it right now (or crashed mid-way and the TTL has not expired).
     */
    IN_PROGRESS,
    /**
     * Already processed successfully; skip it.
     */
    DUPLICATE
}
