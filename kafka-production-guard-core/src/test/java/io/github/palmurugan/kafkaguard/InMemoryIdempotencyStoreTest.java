package io.github.palmurugan.kafkaguard;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

public class InMemoryIdempotencyStoreTest {

    private final InMemoryIdempotencyStore store = new InMemoryIdempotencyStore();

    @Test
    void secondClaimWhileInProgressIsRejected() {
        assertThat(store.claim("k", Duration.ofMinutes(1))).isEqualTo(ClaimResult.ACQUIRED);
        assertThat(store.claim("k", Duration.ofMinutes(1))).isEqualTo(ClaimResult.IN_PROGRESS);
    }

    @Test
    void completedKeyIsDuplicate() {
        store.claim("k", Duration.ofMinutes(1));
        store.complete("k", Duration.ofDays(1));
        assertThat(store.claim("k", Duration.ofMinutes(1))).isEqualTo(ClaimResult.DUPLICATE);
    }

    @Test
    void releasedKeyCanBeReacquired() {
        store.claim("k", Duration.ofMinutes(1));
        store.release("k");
        assertThat(store.claim("k", Duration.ofMinutes(1))).isEqualTo(ClaimResult.ACQUIRED);
    }

    @Test
    void expiredClaimCanBeReacquired() throws InterruptedException {
        store.claim("k", Duration.ofMillis(20));
        Thread.sleep(50);
        assertThat(store.claim("k", Duration.ofMinutes(1))).isEqualTo(ClaimResult.ACQUIRED);
    }
}
