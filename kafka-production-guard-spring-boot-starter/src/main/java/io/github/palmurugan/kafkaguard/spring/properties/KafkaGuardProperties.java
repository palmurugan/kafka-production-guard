package io.github.palmurugan.kafkaguard.spring.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;
import java.util.List;

@ConfigurationProperties(prefix = "kafka.guard")
public record KafkaGuardProperties(@DefaultValue("true") boolean enabled,
                                   @DefaultValue Idempotency idempotency,
                                   @DefaultValue Retry retry,
                                   @DefaultValue Dlt dlt) {

    /**
     * {@code AUTO} prefers a distributed store when one is available (Redis, then JDBC) and
     * falls back to the in-memory store otherwise. Selecting {@code REDIS} or {@code JDBC}
     * explicitly requires the matching module and driver bean to be present, and fails fast at
     * startup otherwise instead of silently degrading to the in-memory store.
     */
    public enum StoreType { AUTO, MEMORY, REDIS, JDBC }

    public record Idempotency(
            @DefaultValue("true") boolean enabled,
            @DefaultValue("idempotency-key") String headerName,
            @DefaultValue("60s") Duration inProgressTtl,
            @DefaultValue("7d") Duration retention,
            @DefaultValue("auto") StoreType storeType) {
    }

    public record Retry(
            @DefaultValue("5") int maxAttempts,
            @DefaultValue("1s") Duration initialInterval,
            @DefaultValue("2.0") double multiplier,
            @DefaultValue("30s") Duration maxInterval,
            List<Class<? extends Throwable>> nonRetryableExceptions) {

        public List<Class<? extends Throwable>> nonRetryableOrEmpty() {
            return nonRetryableExceptions == null ? List.of() : nonRetryableExceptions;
        }
    }

    public record Dlt(
            @DefaultValue("true") boolean enabled,
            @DefaultValue(".DLT") String suffix) {
    }

}
