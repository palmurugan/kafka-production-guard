package io.github.palmurugan.kafkaguard.spring;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;
import java.util.List;

@ConfigurationProperties(prefix = "kafka.guard")
public record KafkaGuardProperties(@DefaultValue("true") boolean enabled,
                                   @DefaultValue Idempotency idempotency,
                                   @DefaultValue Retry retry,
                                   @DefaultValue Dlt dlt) {
    public record Idempotency(
            @DefaultValue("true") boolean enabled,
            @DefaultValue("idempotency-key") String headerName,
            @DefaultValue("60s") Duration inProgressTtl,
            @DefaultValue("7d") Duration retention) {
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
