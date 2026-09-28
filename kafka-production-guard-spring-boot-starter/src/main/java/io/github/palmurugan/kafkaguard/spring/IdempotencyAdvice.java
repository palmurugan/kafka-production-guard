package io.github.palmurugan.kafkaguard.spring;

import io.github.palmurugan.kafkaguard.ClaimResult;
import io.github.palmurugan.kafkaguard.ConcurrentProcessingException;
import io.github.palmurugan.kafkaguard.IdempotencyKeyResolver;
import io.github.palmurugan.kafkaguard.IdempotencyStore;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * Wraps a single-record listener invocation:
 * claim -> (skip | fail-retryable | process) -> complete / release.
 * Batch listeners are passed through untouched in v0.1.
 */
public class IdempotencyAdvice implements MethodInterceptor {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyAdvice.class);

    private final IdempotencyStore store;
    private final IdempotencyKeyResolver resolver;
    private final Supplier<String> groupId;
    private final Duration inProgressTtl;
    private final Duration retention;
    private final GuardMetrics metrics;

    public IdempotencyAdvice(IdempotencyStore store, IdempotencyKeyResolver resolver,
                             Supplier<String> groupId, Duration inProgressTtl,
                             Duration retention, GuardMetrics metrics) {
        this.store = store;
        this.resolver = resolver;
        this.groupId = groupId;
        this.inProgressTtl = inProgressTtl;
        this.retention = retention;
        this.metrics = metrics;
    }

    @Override
    public Object invoke(MethodInvocation invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        if (args.length == 0 || !(args[0] instanceof ConsumerRecord<?, ?> record)) {
            return invocation.proceed();
        }
        String id = resolver.resolve(record);
        if (id == null) {
            if (log.isTraceEnabled()) {
                log.trace("No idempotency key resolved for record in topic {}, skipping guard", record.topic());
            }
            return invocation.proceed();
        }
        // scoped per consumer group: two groups may legitimately process the same message
        String key = groupId.get() + "|" + id;

        ClaimResult claim = store.claim(key, inProgressTtl);
        switch (claim) {
            case DUPLICATE -> {
                if (log.isDebugEnabled()) {
                    log.debug("Skipping duplicate record with key '{}' in topic {}", key, record.topic());
                }
                metrics.duplicateSkipped(record.topic());
                return null; // returning normally => offset is committed, record skipped
            }
            case IN_PROGRESS -> {
                if (log.isDebugEnabled()) {
                    log.debug("Record with key '{}' already in progress in topic {}, will retry", key, record.topic());
                }
                metrics.inProgressConflict(record.topic());
                throw new ConcurrentProcessingException(key); // retried with backoff by the error handler
            }
            case ACQUIRED -> {
                if (log.isTraceEnabled()) {
                    log.trace("Acquired claim for key '{}' in topic {}", key, record.topic());
                }
                /* fall through */
            }
        }

        try {
            Object result = invocation.proceed();
            store.complete(key, retention);
            if (log.isTraceEnabled()) {
                log.trace("Successfully processed record with key '{}' in topic {}", key, record.topic());
            }
            return result;
        } catch (Throwable t) {
            store.release(key);
            if (log.isDebugEnabled()) {
                log.debug("Released claim for key '{}' in topic {} due to exception: {}", key, record.topic(), t.getMessage());
            }
            throw t;
        }
    }
}
