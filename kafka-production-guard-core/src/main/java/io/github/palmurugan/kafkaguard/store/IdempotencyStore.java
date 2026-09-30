package io.github.palmurugan.kafkaguard.store;

import io.github.palmurugan.kafkaguard.enums.ClaimResult;

import java.time.Duration;

/**
 * Storage SPI for idempotency state. Implementations MUST make {@link #claim} atomic
 * across all application instances.
 */
public interface IdempotencyStore {

    /** Atomically claim the key. An expired claim/completion may be re-acquired. */
    ClaimResult claim(String key, Duration inProgressTtl);

    /** Mark the key as successfully processed and keep it for {@code retention}. */
    void complete(String key, Duration retention);

    /** Give up the claim after a failure so a retry can re-acquire it. */
    void release(String key);
}
