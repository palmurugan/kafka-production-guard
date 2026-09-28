package io.github.palmurugan.kafkaguard.spring;

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Owns the producers used for dead-lettering (closed on shutdown). Deliberately NOT exposed as
 * KafkaTemplate/ProducerFactory beans, so it never interferes with Boot's own conditional beans.
 * <ul>
 *   <li>byte[] template: for records that failed deserialization (raw bytes are republished)</li>
 *   <li>default template: uses the application's own configured serializers</li>
 * </ul>
 */
public final class DltTemplates implements DisposableBean {

    private final DefaultKafkaProducerFactory<Object, Object> bytesFactory;
    private final DefaultKafkaProducerFactory<Object, Object> defaultFactory;
    private final Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();

    public DltTemplates(KafkaProperties kafkaProperties, SslBundles sslBundles) {
        Map<String, Object> config = kafkaProperties.buildProducerProperties(sslBundles);

        // passes byte[] through and tolerates String keys that deserialized fine
        DelegatingByTypeSerializer passThrough = new DelegatingByTypeSerializer(Map.of(
                byte[].class, new ByteArraySerializer(),
                String.class, new StringSerializer()));

        this.bytesFactory = new DefaultKafkaProducerFactory<>(config, passThrough, passThrough);
        this.defaultFactory = new DefaultKafkaProducerFactory<>(config);

        // order matters: byte[] first, Object.class is the catch-all
        templates.put(byte[].class, new KafkaTemplate<>(bytesFactory));
        templates.put(Object.class, new KafkaTemplate<>(defaultFactory));
    }

    public Map<Class<?>, KafkaOperations<?, ?>> templates() {
        return templates;
    }

    @Override
    public void destroy() {
        bytesFactory.destroy();
        defaultFactory.destroy();
    }
}
