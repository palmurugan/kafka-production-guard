package io.github.palmurugan.kafkaguard.jdbc.store;

import io.github.palmurugan.kafkaguard.enums.ClaimResult;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.postgresql.Driver;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the store works against a real PostgreSQL server, using the exact, unmodified DDL from
 * the root {@code scripts.sql}. No Postgres-specific code exists in {@link JdbcIdempotencyStore};
 * this test validates the generic implementation, not a dialect-specific one.
 */
@Testcontainers
class JdbcIdempotencyStorePostgresIT {

    private static final String TABLE = "kafka_guard_idempotency";

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    private static JdbcTemplate jdbc;
    private JdbcIdempotencyStore store;

    @BeforeAll
    static void setUpDatabase() {
        SimpleDriverDataSource dataSource = new SimpleDriverDataSource(new Driver(),
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        jdbc = new JdbcTemplate(dataSource);
        jdbc.execute("""
                CREATE TABLE kafka_guard_idempotency (
                    idempotency_key VARCHAR(255) NOT NULL PRIMARY KEY,
                    status          VARCHAR(16)  NOT NULL,
                    expires_at      TIMESTAMP    NOT NULL
                )
                """);
        jdbc.execute("CREATE INDEX idx_kafka_guard_idem_expires ON kafka_guard_idempotency (expires_at)");
    }

    @BeforeEach
    void setUp() {
        jdbc.update("DELETE FROM " + TABLE);
        store = new JdbcIdempotencyStore(jdbc, TABLE);
    }

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

    @Test
    @Timeout(30)
    void onlyOneOfManyConcurrentClaimersAcquires() throws Exception {
        int threads = 20;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            Callable<ClaimResult> task = () -> {
                barrier.await();
                return store.claim("concurrent-key", Duration.ofMinutes(1));
            };
            List<Future<ClaimResult>> futures = pool.invokeAll(Collections.nCopies(threads, task));

            long acquired = 0;
            long inProgress = 0;
            for (Future<ClaimResult> future : futures) {
                ClaimResult result = future.get();
                if (result == ClaimResult.ACQUIRED) {
                    acquired++;
                } else if (result == ClaimResult.IN_PROGRESS) {
                    inProgress++;
                }
            }
            assertThat(acquired).isEqualTo(1);
            assertThat(inProgress).isEqualTo(threads - 1);
        } finally {
            pool.shutdownNow();
        }
    }
}
