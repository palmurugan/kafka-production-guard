package io.github.palmurugan.kafkaguard;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;

/**
 * Derives the idempotency key for a record. Return null to skip deduplication for it.
 */
@FunctionalInterface
public interface IdempotencyKeyResolver {

    String resolve(ConsumerRecord<?, ?> record);

    /**
     * Uses the given header when present; otherwise falls back to topic-partition@offset.
     * The fallback only protects against redelivery of the same log entry (e.g. after a
     * rebalance), not against duplicates re-published by a producer. Prefer a business id header.
     */
    static IdempotencyKeyResolver headerOrCoordinates(String headerName) {
        return record -> {
            Header h = record.headers().lastHeader(headerName);
            if (h != null && h.value() != null) {
                return new String(h.value(), StandardCharsets.UTF_8);
            }
            return record.topic() + "-" + record.partition() + "@" + record.offset();
        };
    }
}
