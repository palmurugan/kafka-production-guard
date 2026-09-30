# Kafka Production Guard

Production-grade Kafka consumer safety for Spring Boot: idempotency, poison-message handling, and more.

## Features

- **Idempotency**: Prevents duplicate message processing across multiple application instances
- **Poison Message Handling**: Automatically detects and routes non-retryable failures to Dead Letter Topics (DLT)
- **Exponential Backoff Retry**: Configurable retry with backoff for transient failures
- **Metrics Integration**: Micrometer metrics for monitoring duplicates, conflicts, and dead-lettered messages
- **Flexible Storage**: In-memory store for development, Redis or JDBC (including PostgreSQL) for production

## Quick Start

### Add Dependency

```xml
<dependency>
    <groupId>io.github.palmurugan</groupId>
    <artifactId>kafka-production-guard-spring-boot-starter</artifactId>
    <version>0.1.0</version>
</dependency>
```

For production with Redis backing (recommended default):

```xml
<dependency>
    <groupId>io.github.palmurugan</groupId>
    <artifactId>kafka-production-guard-redis</artifactId>
    <version>0.1.0</version>
</dependency>
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-data-redis</artifactId>
</dependency>
```

Or with JDBC backing (any JDBC datasource, including PostgreSQL):

```xml
<dependency>
    <groupId>io.github.palmurugan</groupId>
    <artifactId>kafka-production-guard-jdbc</artifactId>
    <version>0.1.0</version>
</dependency>
```

### Configuration

```yaml
kafka:
  guard:
    enabled: true
    idempotency:
      enabled: true
      header-name: idempotency-key
      in-progress-ttl: 60s
      retention: 7d
      store-type: auto # auto | memory | redis | jdbc
    retry:
      max-attempts: 5
      initial-interval: 1s
      multiplier: 2.0
      max-interval: 30s
    dlt:
      enabled: true
      suffix: .DLT
```

`store-type` controls which `IdempotencyStore` is used:

- `auto` (default): prefers Redis, then JDBC, then falls back to the in-memory store, based on what's on the classpath and configured. Backward compatible — with neither Redis nor JDBC configured, behavior is unchanged (in-memory).
- `redis` / `jdbc`: pins the store explicitly. If the matching module isn't on the classpath or its driver bean (`StringRedisTemplate` / `JdbcTemplate`) isn't available, startup fails fast with a clear error instead of silently falling back to in-memory.
- `memory`: always uses the in-memory store, even if Redis/JDBC are available. Not safe across multiple instances — for development/testing only.

### Redis Setup (Redis Store)

Bring your own `StringRedisTemplate`/`RedisConnectionFactory` bean, typically via `spring-boot-starter-data-redis`:

```yaml
spring:
  data:
    redis:
      host: localhost
      port: 6379
```

Optionally configure the key prefix used for idempotency entries:

```yaml
kafka:
  guard:
    idempotency:
      redis:
        key-prefix: "kafka-guard:idempotency:"
```

### Database Setup (JDBC Store)

Create the idempotency table:

```sql
CREATE TABLE kafka_guard_idempotency (
    idempotency_key VARCHAR(255) NOT NULL PRIMARY KEY,
    status VARCHAR(16) NOT NULL,
    expires_at TIMESTAMP NOT NULL
);
CREATE INDEX idx_kafka_guard_idem_expires ON kafka_guard_idempotency (expires_at);
```

Optionally configure table name:

```yaml
kafka:
  guard:
    idempotency:
      jdbc:
        table: kafka_guard_idempotency
```

This schema and store implementation are plain ANSI SQL and work unmodified against PostgreSQL — validated by a Testcontainers-backed integration test (`kafka-production-guard-jdbc`). To use PostgreSQL, add a `DataSource` on the app side:

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/mydb
    driver-class-name: org.postgresql.Driver
```

```xml
<dependency>
    <groupId>org.postgresql</groupId>
    <artifactId>postgresql</artifactId>
    <scope>runtime</scope>
</dependency>
```

## Usage

### Idempotency Key Resolution

By default, the guard uses the `idempotency-key` header if present, otherwise falls back to `topic-partition@offset`.

Add a business key header to your messages:

```java
ProducerRecord<String, String> record = new ProducerRecord<>("my-topic", "value");
record.headers().add("idempotency-key", "order-12345".getBytes());
kafkaTemplate.send(record);
```

### Non-Retryable Exceptions

Extend `NonRetryableException` for failures that should go straight to DLT:

```java
public class ValidationException extends NonRetryableException {
    public ValidationException(String message) {
        super(message);
    }
}
```

Or configure additional exception types:

```yaml
kafka:
  guard:
    retry:
      non-retryable-exceptions:
        - com.example.ValidationException
        - org.springframework.dao.DataIntegrityViolationException
```

### Built-in Non-Retryable Exceptions

The following exceptions are automatically treated as poison (non-retryable):
- `DeserializationException`
- `MessageConversionException`
- `ConversionException`
- `MethodArgumentResolutionException`
- `ClassCastException`
- `NonRetryableException`

## Metrics

When Micrometer is available, the following metrics are published:

- `kafka.guard.duplicates.skipped` - Count of duplicate messages skipped
- `kafka.guard.inprogress.conflicts` - Count of concurrent processing conflicts
- `kafka.guard.dead.lettered` - Count of messages sent to DLT (with tags: topic, type)

## Modules

- **kafka-production-guard-core**: Core interfaces and in-memory implementation
- **kafka-production-guard-spring-boot-starter**: Spring Boot auto-configuration
- **kafka-production-guard-jdbc**: JDBC-backed idempotency store for production (any JDBC datasource, including PostgreSQL)
- **kafka-production-guard-redis**: Redis-backed idempotency store for production

## Testing

Unit tests run with plain `mvn test`. Integration tests for the Redis and JDBC/PostgreSQL stores use [Testcontainers](https://testcontainers.com/) and require Docker to be running locally; they run automatically as part of `mvn test`/`mvn verify` in the `kafka-production-guard-redis` and `kafka-production-guard-jdbc` modules. No CI pipeline is configured for them yet.

## License

Apache License 2.0