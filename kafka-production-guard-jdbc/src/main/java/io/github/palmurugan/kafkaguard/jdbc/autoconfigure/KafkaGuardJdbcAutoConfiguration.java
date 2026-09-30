package io.github.palmurugan.kafkaguard.jdbc.autoconfigure;

import io.github.palmurugan.kafkaguard.jdbc.store.JdbcIdempotencyStore;
import io.github.palmurugan.kafkaguard.store.IdempotencyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;

@AutoConfiguration(
        after = JdbcTemplateAutoConfiguration.class,
        beforeName = "io.github.palmurugan.kafkaguard.spring.autoconfigure.KafkaGuardAutoConfiguration")
@ConditionalOnClass(JdbcTemplate.class)
@ConditionalOnBean(JdbcTemplate.class)
public class KafkaGuardJdbcAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(KafkaGuardJdbcAutoConfiguration.class);

    @Bean
    @ConditionalOnMissingBean(IdempotencyStore.class)
    @ConditionalOnExpression("'${kafka.guard.idempotency.store-type:auto}'.toLowerCase().matches('^(auto|jdbc)$')")
    JdbcIdempotencyStore jdbcIdempotencyStore(
            JdbcTemplate jdbc,
            @Value("${kafka.guard.idempotency.jdbc.table:kafka_guard_idempotency}") String table) {
        log.info("kafka-production-guard: JDBC idempotency store configured with table '{}'", table);
        return new JdbcIdempotencyStore(jdbc, table);
    }
}
