package io.github.palmurugan.kafkaguard.redis.autoconfigure;

import io.github.palmurugan.kafkaguard.redis.store.RedisIdempotencyStore;
import io.github.palmurugan.kafkaguard.store.IdempotencyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.StringRedisTemplate;

@AutoConfiguration(beforeName = {
        "io.github.palmurugan.kafkaguard.jdbc.autoconfigure.KafkaGuardJdbcAutoConfiguration",
        "io.github.palmurugan.kafkaguard.spring.autoconfigure.KafkaGuardAutoConfiguration"})
@ConditionalOnClass(StringRedisTemplate.class)
@ConditionalOnBean(StringRedisTemplate.class)
public class KafkaGuardRedisAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(KafkaGuardRedisAutoConfiguration.class);

    @Bean
    @ConditionalOnMissingBean(IdempotencyStore.class)
    @ConditionalOnExpression("'${kafka.guard.idempotency.store-type:auto}'.toLowerCase().matches('^(auto|redis)$')")
    RedisIdempotencyStore redisIdempotencyStore(
            StringRedisTemplate redisTemplate,
            @Value("${kafka.guard.idempotency.redis.key-prefix:kafka-guard:idempotency:}") String keyPrefix) {
        log.info("kafka-production-guard: Redis idempotency store configured with key prefix '{}'", keyPrefix);
        return new RedisIdempotencyStore(redisTemplate, keyPrefix);
    }
}
