package io.github.palmurugan.kafkaguard.spring;

import io.micrometer.core.instrument.MeterRegistry;

public class MicrometerGuardMetrics implements GuardMetrics {

    private final MeterRegistry registry;

    public MicrometerGuardMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void duplicateSkipped(String topic) {
        registry.counter("kafka.guard.duplicates.skipped", "topic", topic).increment();
    }

    @Override
    public void inProgressConflict(String topic) {
        registry.counter("kafka.guard.inprogress.conflicts", "topic", topic).increment();
    }

    @Override
    public void deadLettered(String topic, String failureType) {
        registry.counter("kafka.guard.dead.lettered", "topic", topic, "type", failureType).increment();
    }
}
