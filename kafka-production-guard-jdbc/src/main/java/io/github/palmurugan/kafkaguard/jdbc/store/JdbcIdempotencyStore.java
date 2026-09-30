package io.github.palmurugan.kafkaguard.jdbc.store;

import io.github.palmurugan.kafkaguard.enums.ClaimResult;
import io.github.palmurugan.kafkaguard.store.IdempotencyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.regex.Pattern;

/**
 * Atomicity comes from the primary key: INSERT wins for exactly one caller. An expired row is
 * taken over with a conditional UPDATE. Each call autocommits on its own connection, which also
 * avoids PostgreSQL's "transaction aborted after duplicate key" behaviour.
 */
public class JdbcIdempotencyStore implements IdempotencyStore {

    private static final Logger log = LoggerFactory.getLogger(JdbcIdempotencyStore.class);
    private static final Pattern SAFE_TABLE = Pattern.compile("[A-Za-z0-9_.]+");

    private final JdbcTemplate jdbc;
    private final String table;

    public JdbcIdempotencyStore(JdbcTemplate jdbc, String table) {
        if (!SAFE_TABLE.matcher(table).matches()) {
            throw new IllegalArgumentException("Illegal table name: " + table);
        }
        this.jdbc = jdbc;
        this.table = table;
        log.info("kafka-production-guard: JDBC idempotency store initialized with table '{}'", table);
    }

    @Override
    public ClaimResult claim(String key, Duration inProgressTtl) {
        Instant now = Instant.now();
        Timestamp expiry = Timestamp.from(now.plus(inProgressTtl));
        try {
            jdbc.update("INSERT INTO " + table + " (idempotency_key, status, expires_at) VALUES (?, 'IN_PROGRESS', ?)",
                    key, expiry);
            if (log.isTraceEnabled()) {
                log.trace("Acquired claim for key '{}' with TTL {}", key, inProgressTtl);
            }
            return ClaimResult.ACQUIRED;
        } catch (DuplicateKeyException e) {
            int taken = jdbc.update("UPDATE " + table + " SET status = 'IN_PROGRESS', expires_at = ? "
                    + "WHERE idempotency_key = ? AND expires_at < ?", expiry, key, Timestamp.from(now));
            if (taken == 1) {
                if (log.isTraceEnabled()) {
                    log.trace("Reacquired expired claim for key '{}'", key);
                }
                return ClaimResult.ACQUIRED;
            }
            String status = jdbc.query("SELECT status FROM " + table + " WHERE idempotency_key = ?",
                    rs -> rs.next() ? rs.getString(1) : null, key);
            ClaimResult result = "COMPLETED".equals(status) ? ClaimResult.DUPLICATE : ClaimResult.IN_PROGRESS;
            if (log.isDebugEnabled()) {
                log.debug("Claim result for key '{}': {} (current status: {})", key, result, status);
            }
            return result;
        }
    }

    @Override
    public void complete(String key, Duration retention) {
        int updated = jdbc.update("UPDATE " + table + " SET status = 'COMPLETED', expires_at = ? WHERE idempotency_key = ?",
                Timestamp.from(Instant.now().plus(retention)), key);
        if (log.isDebugEnabled()) {
            log.debug("Marked key '{}' as completed with retention {} (updated {} rows)", key, retention, updated);
        }
    }

    @Override
    public void release(String key) {
        int deleted = jdbc.update("DELETE FROM " + table + " WHERE idempotency_key = ? AND status = 'IN_PROGRESS'", key);
        if (log.isDebugEnabled()) {
            log.debug("Released claim for key '{}' (deleted {} rows)", key, deleted);
        }
    }

    /**
     * Call periodically (e.g. from a @Scheduled method). Returns the number of rows removed.
     */
    public int purgeExpired() {
        int purged = jdbc.update("DELETE FROM " + table + " WHERE expires_at < ?", Timestamp.from(Instant.now()));
        if (purged > 0) {
            log.info("Purged {} expired entries from table '{}'", purged, table);
        }
        return purged;
    }
}
