package io.github.palmurugan.kafkaguard.redis.store;

import io.github.palmurugan.kafkaguard.enums.ClaimResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

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
 * Proves the Lua-script-based claim/release semantics of {@link RedisIdempotencyStore} against a
 * real Redis server (not a mock), including the concurrent-claim atomicity guarantee.
 */
@Testcontainers
class RedisIdempotencyStoreIT {

    private static final String PREFIX = "test:idempotency:";

    @Container
    private static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    private static LettuceConnectionFactory connectionFactory;
    private static StringRedisTemplate redisTemplate;
    private RedisIdempotencyStore store;

    @BeforeAll
    static void setUpConnection() {
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration(
                REDIS.getHost(), REDIS.getMappedPort(6379));
        connectionFactory = new LettuceConnectionFactory(config);
        connectionFactory.afterPropertiesSet();
        redisTemplate = new StringRedisTemplate(connectionFactory);
        redisTemplate.afterPropertiesSet();
    }

    @AfterAll
    static void tearDownConnection() {
        connectionFactory.destroy();
    }

    @BeforeEach
    void setUp() {
        redisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
        store = new RedisIdempotencyStore(redisTemplate, PREFIX);
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
        Thread.sleep(100);
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
