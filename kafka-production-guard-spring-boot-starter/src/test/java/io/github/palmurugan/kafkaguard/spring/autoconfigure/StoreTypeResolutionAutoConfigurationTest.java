package io.github.palmurugan.kafkaguard.spring.autoconfigure;

import io.github.palmurugan.kafkaguard.store.IdempotencyStore;
import io.github.palmurugan.kafkaguard.store.InMemoryIdempotencyStore;
import io.github.palmurugan.kafkaguard.jdbc.autoconfigure.KafkaGuardJdbcAutoConfiguration;
import io.github.palmurugan.kafkaguard.jdbc.store.JdbcIdempotencyStore;
import io.github.palmurugan.kafkaguard.redis.autoconfigure.KafkaGuardRedisAutoConfiguration;
import io.github.palmurugan.kafkaguard.redis.store.RedisIdempotencyStore;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Verifies the end-to-end {@code kafka.guard.idempotency.store-type} resolution matrix across the
 * three independently-conditional store autoconfigurations: {@code auto} prefers Redis over JDBC
 * over in-memory, an explicit selection wins even when a higher-priority store is also available,
 * and an explicit REDIS/JDBC selection without the matching driver bean fails fast at startup
 * instead of silently falling back to the in-memory store.
 */
class StoreTypeResolutionAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withPropertyValues("kafka.guard.dlt.enabled=false")
            .withConfiguration(AutoConfigurations.of(
                    KafkaGuardRedisAutoConfiguration.class,
                    KafkaGuardJdbcAutoConfiguration.class,
                    KafkaGuardAutoConfiguration.class));

    @Test
    void autoWithOnlyJdbcAvailablePicksJdbc() {
        contextRunner.withBean(JdbcTemplate.class, () -> mock(JdbcTemplate.class))
                .run(context -> assertThat(context).getBean(IdempotencyStore.class)
                        .isInstanceOf(JdbcIdempotencyStore.class));
    }

    @Test
    void autoWithBothAvailablePicksRedis() {
        contextRunner.withBean(JdbcTemplate.class, () -> mock(JdbcTemplate.class))
                .withBean(StringRedisTemplate.class, () -> mock(StringRedisTemplate.class))
                .run(context -> assertThat(context).getBean(IdempotencyStore.class)
                        .isInstanceOf(RedisIdempotencyStore.class));
    }

    @Test
    void autoWithNeitherAvailablePicksInMemory() {
        contextRunner.run(context -> assertThat(context).getBean(IdempotencyStore.class)
                .isInstanceOf(InMemoryIdempotencyStore.class));
    }

    @Test
    void explicitJdbcSkipsRedisEvenWhenAvailable() {
        contextRunner.withPropertyValues("kafka.guard.idempotency.store-type=jdbc")
                .withBean(JdbcTemplate.class, () -> mock(JdbcTemplate.class))
                .withBean(StringRedisTemplate.class, () -> mock(StringRedisTemplate.class))
                .run(context -> assertThat(context).getBean(IdempotencyStore.class)
                        .isInstanceOf(JdbcIdempotencyStore.class));
    }

    @Test
    void explicitMemoryIgnoresAvailableStores() {
        contextRunner.withPropertyValues("kafka.guard.idempotency.store-type=memory")
                .withBean(JdbcTemplate.class, () -> mock(JdbcTemplate.class))
                .withBean(StringRedisTemplate.class, () -> mock(StringRedisTemplate.class))
                .run(context -> assertThat(context).getBean(IdempotencyStore.class)
                        .isInstanceOf(InMemoryIdempotencyStore.class));
    }

    @Test
    void explicitRedisWithoutRedisBeanFailsFast() {
        contextRunner.withPropertyValues("kafka.guard.idempotency.store-type=redis")
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void explicitJdbcWithoutJdbcBeanFailsFast() {
        contextRunner.withPropertyValues("kafka.guard.idempotency.store-type=jdbc")
                .run(context -> assertThat(context).hasFailed());
    }
}
