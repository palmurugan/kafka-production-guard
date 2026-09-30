package io.github.palmurugan.kafkaguard.spring.autoconfigure;

import io.github.palmurugan.kafkaguard.resolver.IdempotencyKeyResolver;
import io.github.palmurugan.kafkaguard.spring.advice.IdempotencyAdvice;
import io.github.palmurugan.kafkaguard.spring.classifier.FailureClassifier;
import io.github.palmurugan.kafkaguard.spring.dlt.DltTemplates;
import io.github.palmurugan.kafkaguard.spring.metrics.GuardMetrics;
import io.github.palmurugan.kafkaguard.spring.metrics.MicrometerGuardMetrics;
import io.github.palmurugan.kafkaguard.spring.properties.KafkaGuardProperties;
import io.github.palmurugan.kafkaguard.store.IdempotencyStore;
import io.github.palmurugan.kafkaguard.store.InMemoryIdempotencyStore;
import io.micrometer.core.instrument.MeterRegistry;
import org.aopalliance.aop.Advice;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

@AutoConfiguration(
        after = KafkaAutoConfiguration.class,
        afterName = "org.springframework.boot.actuate.autoconfigure.metrics.CompositeMeterRegistryAutoConfiguration")
@ConditionalOnClass(KafkaTemplate.class)
@ConditionalOnProperty(prefix = "kafka.guard", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(KafkaGuardProperties.class)
public class KafkaGuardAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(KafkaGuardAutoConfiguration.class);

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(MeterRegistry.class)
    static class MicrometerConfiguration {
        @Bean
        @ConditionalOnBean(MeterRegistry.class)
        @ConditionalOnMissingBean(GuardMetrics.class)
        GuardMetrics micrometerGuardMetrics(MeterRegistry registry) {
            return new MicrometerGuardMetrics(registry);
        }
    }

    @Bean
    @ConditionalOnMissingBean(GuardMetrics.class)
    GuardMetrics noopGuardMetrics() {
        log.info("kafka-production-guard: using NOOP metrics (Micrometer not available)");
        return GuardMetrics.NOOP;
    }

    /**
     * Runs only when no upstream store (Redis, JDBC) claimed the {@link IdempotencyStore} bean.
     * For an explicit REDIS/JDBC {@code store-type}, that means the required module/bean is
     * missing, so this fails fast with a clear remediation message instead of silently degrading
     * to the in-memory store.
     */
    @Bean
    @ConditionalOnMissingBean(IdempotencyStore.class)
    IdempotencyStore fallbackIdempotencyStore(KafkaGuardProperties props) {
        KafkaGuardProperties.StoreType storeType = props.idempotency().storeType();
        if (storeType == KafkaGuardProperties.StoreType.REDIS) {
            throw new IllegalStateException("kafka.guard.idempotency.store-type=redis was requested, but no "
                    + "IdempotencyStore could be configured for Redis. Add kafka-production-guard-redis to the "
                    + "classpath and ensure a StringRedisTemplate bean is available (e.g. "
                    + "spring-boot-starter-data-redis with spring.data.redis.* configured).");
        }
        if (storeType == KafkaGuardProperties.StoreType.JDBC) {
            throw new IllegalStateException("kafka.guard.idempotency.store-type=jdbc was requested, but no "
                    + "IdempotencyStore could be configured for JDBC. Add kafka-production-guard-jdbc to the "
                    + "classpath and ensure a JdbcTemplate bean is available.");
        }
        log.warn("kafka-production-guard: using IN-MEMORY idempotency store. This is NOT safe with multiple instances. "
                + "Add kafka-production-guard-jdbc or kafka-production-guard-redis, or provide your own "
                + "IdempotencyStore bean, for production use.");
        return new InMemoryIdempotencyStore();
    }

    @Bean
    @ConditionalOnMissingBean(IdempotencyKeyResolver.class)
    IdempotencyKeyResolver idempotencyKeyResolver(KafkaGuardProperties props) {
        String headerName = props.idempotency().headerName();
        log.info("kafka-production-guard: using header '{}' for idempotency key resolution", headerName);
        return IdempotencyKeyResolver.headerOrCoordinates(headerName);
    }

    @Bean
    @ConditionalOnMissingBean
    FailureClassifier failureClassifier(KafkaGuardProperties props) {
        var extra = props.retry().nonRetryableOrEmpty();
        if (!extra.isEmpty()) {
            log.info("kafka-production-guard: registered {} additional non-retryable exception types", extra.size());
        }
        return new FailureClassifier(extra);
    }

    @Bean
    @ConditionalOnProperty(prefix = "kafka.guard.dlt", name = "enabled", havingValue = "true", matchIfMissing = true)
    @ConditionalOnMissingBean
    DltTemplates kafkaGuardDltTemplates(KafkaProperties kafkaProperties, ObjectProvider<SslBundles> sslBundles) {
        log.info("kafka-production-guard: DLT templates initialized");
        return new DltTemplates(kafkaProperties, sslBundles.getIfAvailable());
    }

    @Bean
    @ConditionalOnMissingBean(CommonErrorHandler.class)
    DefaultErrorHandler kafkaGuardErrorHandler(KafkaGuardProperties props,
                                               FailureClassifier classifier,
                                               ObjectProvider<DltTemplates> dltTemplates,
                                               GuardMetrics metrics) {
        KafkaGuardProperties.Retry retry = props.retry();
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(retry.maxAttempts());
        backOff.setInitialInterval(retry.initialInterval().toMillis());
        backOff.setMultiplier(retry.multiplier());
        backOff.setMaxInterval(retry.maxInterval().toMillis());

        DltTemplates templates = dltTemplates.getIfAvailable();
        DefaultErrorHandler handler;
        if (templates == null) {
            log.info("kafka-production-guard: error handler configured without DLT (max attempts: {})", retry.maxAttempts());
            handler = new DefaultErrorHandler(backOff); // logs and skips after retries are exhausted
        } else {
            String suffix = props.dlt().suffix();
            log.info("kafka-production-guard: error handler configured with DLT suffix '{}' (max attempts: {})", suffix, retry.maxAttempts());
            DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                    templates.templates(),
                    // partition -1 => producer picks (by key); avoids failures when the DLT has
                    // fewer partitions than the source topic
                    (record, ex) -> new TopicPartition(record.topic() + suffix, -1));
            recoverer.setHeadersFunction((record, ex) -> {
                String type = classifier.isPoison(ex) ? "POISON" : "RETRIES_EXHAUSTED";
                metrics.deadLettered(record.topic(), type);
                Headers headers = new RecordHeaders();
                headers.add("kpg-failure-type", type.getBytes(StandardCharsets.UTF_8));
                headers.add("kpg-failed-at", Instant.now().toString().getBytes(StandardCharsets.UTF_8));
                return headers;
            });
            handler = new DefaultErrorHandler(recoverer, backOff);
        }
        handler.addNotRetryableExceptions(classifier.nonRetryable().toArray(new Class[0]));
        return handler;
    }

    /**
     * Boot's Kafka auto-config applies a single ContainerCustomizer bean of exactly this type
     * to its default listener container factory.
     */
    @Bean
    @ConditionalOnMissingBean(name = "kafkaGuardContainerCustomizer")
    @ConditionalOnProperty(prefix = "kafka.guard.idempotency", name = "enabled", havingValue = "true", matchIfMissing = true)
    ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>> kafkaGuardContainerCustomizer(
            KafkaGuardProperties props, IdempotencyStore store, IdempotencyKeyResolver resolver, GuardMetrics metrics) {
        log.info("kafka-production-guard: idempotency advice enabled for Kafka listener containers");
        return container -> {
            // groupId is resolved lazily: @KafkaListener(groupId=...) may be applied after customization
            Advice guard = new IdempotencyAdvice(store, resolver, container::getGroupId,
                    props.idempotency().inProgressTtl(), props.idempotency().retention(), metrics);
            Advice[] existing = container.getContainerProperties().getAdviceChain();
            Advice[] merged = new Advice[existing == null ? 1 : existing.length + 1];
            if (existing != null) {
                System.arraycopy(existing, 0, merged, 0, existing.length);
            }
            merged[merged.length - 1] = guard;
            container.getContainerProperties().setAdviceChain(merged);
        };
    }
}
