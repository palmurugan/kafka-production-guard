package io.github.palmurugan.kafkaguard.redis.store;

import io.github.palmurugan.kafkaguard.enums.ClaimResult;
import io.github.palmurugan.kafkaguard.store.IdempotencyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import java.time.Duration;
import java.util.List;

/**
 * Atomicity comes from single-key Lua scripts: {@code claim} does a GET-then-SET in one round
 * trip so there's no gap between checking the current status and acquiring the claim, and
 * {@code release} only deletes if the key is still IN_PROGRESS so it can't clobber a claim
 * legitimately re-acquired by another instance after this one's TTL expired. Redis's own key TTL
 * replaces the manual expires-at bookkeeping the JDBC store needs.
 */
public class RedisIdempotencyStore implements IdempotencyStore {

    private static final Logger log = LoggerFactory.getLogger(RedisIdempotencyStore.class);

    private static final RedisScript<String> CLAIM_SCRIPT = new DefaultRedisScript<>("""
            local v = redis.call('GET', KEYS[1])
            if v == false then
              redis.call('SET', KEYS[1], 'IN_PROGRESS', 'PX', ARGV[1])
              return 'ACQUIRED'
            elseif v == 'COMPLETED' then
              return 'DUPLICATE'
            else
              return 'IN_PROGRESS'
            end
            """, String.class);

    private static final RedisScript<Long> RELEASE_SCRIPT = new DefaultRedisScript<>("""
            if redis.call('GET', KEYS[1]) == 'IN_PROGRESS' then
              return redis.call('DEL', KEYS[1])
            end
            return 0
            """, Long.class);

    private final StringRedisTemplate redisTemplate;
    private final String keyPrefix;

    public RedisIdempotencyStore(StringRedisTemplate redisTemplate, String keyPrefix) {
        this.redisTemplate = redisTemplate;
        this.keyPrefix = keyPrefix;
        log.info("kafka-production-guard: Redis idempotency store initialized with key prefix '{}'", keyPrefix);
    }

    @Override
    public ClaimResult claim(String key, Duration inProgressTtl) {
        long ttlMillis = Math.max(1, inProgressTtl.toMillis());
        String outcome = redisTemplate.execute(CLAIM_SCRIPT, List.of(keyPrefix + key), String.valueOf(ttlMillis));
        ClaimResult result = ClaimResult.valueOf(outcome);
        if (log.isTraceEnabled()) {
            log.trace("Claim result for key '{}': {}", key, result);
        }
        return result;
    }

    @Override
    public void complete(String key, Duration retention) {
        // Unconditional SET, unlike the JDBC store's no-op-if-missing UPDATE: this correctly
        // blocks reprocessing during retention even if the IN_PROGRESS key already expired.
        redisTemplate.opsForValue().set(keyPrefix + key, "COMPLETED", retention);
        if (log.isDebugEnabled()) {
            log.debug("Marked key '{}' as completed with retention {}", key, retention);
        }
    }

    @Override
    public void release(String key) {
        Long deleted = redisTemplate.execute(RELEASE_SCRIPT, List.of(keyPrefix + key));
        if (log.isDebugEnabled()) {
            log.debug("Released claim for key '{}' (deleted={})", key, deleted != null && deleted > 0);
        }
    }
}
