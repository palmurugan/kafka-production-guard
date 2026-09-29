CREATE TABLE kafka_guard_idempotency (
                                         idempotency_key VARCHAR(255) NOT NULL PRIMARY KEY,
                                         status          VARCHAR(16)  NOT NULL,
                                         expires_at      TIMESTAMP    NOT NULL
);

CREATE INDEX idx_kafka_guard_idem_expires ON kafka_guard_idempotency (expires_at);