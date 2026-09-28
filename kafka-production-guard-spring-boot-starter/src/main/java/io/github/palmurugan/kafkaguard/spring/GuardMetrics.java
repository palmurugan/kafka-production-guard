package io.github.palmurugan.kafkaguard.spring;

public interface GuardMetrics {
    GuardMetrics NOOP = new GuardMetrics() {
        @Override
        public void duplicateSkipped(String topic) {
        }

        @Override
        public void inProgressConflict(String topic) {
        }

        @Override
        public void deadLettered(String topic, String failureType) {
        }
    };

    void duplicateSkipped(String topic);

    void inProgressConflict(String topic);

    void deadLettered(String topic, String failureType);
}
