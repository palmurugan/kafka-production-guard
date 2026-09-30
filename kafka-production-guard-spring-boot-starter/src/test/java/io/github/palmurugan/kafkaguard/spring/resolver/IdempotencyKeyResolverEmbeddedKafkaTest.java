package io.github.palmurugan.kafkaguard.spring.resolver;

import io.github.palmurugan.kafkaguard.resolver.IdempotencyKeyResolver;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises {@link IdempotencyKeyResolver} against a real embedded broker so header decoding and
 * the topic-partition@offset fallback are checked against genuine broker-assigned coordinates,
 * not fabricated {@link ConsumerRecord} instances. Lives in this module (rather than
 * kafka-production-guard-core) because it already carries the spring-kafka-test dependency.
 */
@EmbeddedKafka(partitions = 2, topics = {
        "header-present-topic", "header-absent-topic", "same-header-topic"
})
class IdempotencyKeyResolverEmbeddedKafkaTest {

    private static final String HEADER_NAME = "idempotency-key";

    private final IdempotencyKeyResolver resolver = IdempotencyKeyResolver.headerOrCoordinates(HEADER_NAME);

    @Test
    void headerPresentReturnsDecodedHeaderValue(EmbeddedKafkaBroker broker) throws Exception {
        String topic = "header-present-topic";
        try (KafkaProducer<String, String> producer = newProducer(broker)) {
            List<Header> headers = List.of(new RecordHeader(HEADER_NAME, "biz-123".getBytes(StandardCharsets.UTF_8)));
            producer.send(new ProducerRecord<>(topic, null, "some-key", "payload", headers))
                    .get(10, TimeUnit.SECONDS);
        }

        try (KafkaConsumer<String, String> consumer = newConsumer(broker, "header-present-group")) {
            consumer.subscribe(List.of(topic));
            ConsumerRecord<String, String> consumed = KafkaTestUtils.getSingleRecord(consumer, topic);

            assertThat(resolver.resolve(consumed)).isEqualTo("biz-123");
        }
    }

    @Test
    void headerAbsentFallsBackToRealTopicPartitionOffset(EmbeddedKafkaBroker broker) throws Exception {
        String topic = "header-absent-topic";
        RecordMetadata meta;
        try (KafkaProducer<String, String> producer = newProducer(broker)) {
            meta = producer.send(new ProducerRecord<>(topic, "some-key", "payload")).get(10, TimeUnit.SECONDS);
        }

        try (KafkaConsumer<String, String> consumer = newConsumer(broker, "header-absent-group")) {
            consumer.subscribe(List.of(topic));
            ConsumerRecord<String, String> consumed = KafkaTestUtils.getSingleRecord(consumer, topic);

            assertThat(consumed.topic()).isEqualTo(meta.topic());
            assertThat(consumed.partition()).isEqualTo(meta.partition());
            assertThat(consumed.offset()).isEqualTo(meta.offset());

            assertThat(resolver.resolve(consumed))
                    .isEqualTo(meta.topic() + "-" + meta.partition() + "@" + meta.offset());
        }
    }

    @Test
    void sameHeaderValueAtDifferentCoordinatesResolvesToSameKey(EmbeddedKafkaBroker broker) throws Exception {
        String topic = "same-header-topic";
        List<Header> headers = List.of(new RecordHeader(HEADER_NAME, "biz-999".getBytes(StandardCharsets.UTF_8)));
        try (KafkaProducer<String, String> producer = newProducer(broker)) {
            producer.send(new ProducerRecord<>(topic, 0, "k1", "v1", headers)).get(10, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(topic, 1, "k2", "v2", headers)).get(10, TimeUnit.SECONDS);
        }

        try (KafkaConsumer<String, String> consumer = newConsumer(broker, "same-header-group")) {
            consumer.subscribe(List.of(topic));
            ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10), 2);

            List<ConsumerRecord<String, String>> consumed = new ArrayList<>();
            records.forEach(consumed::add);
            assertThat(consumed).hasSize(2);

            ConsumerRecord<String, String> first = consumed.get(0);
            ConsumerRecord<String, String> second = consumed.get(1);
            assertThat(first.partition()).isNotEqualTo(second.partition());

            String firstKey = resolver.resolve(first);
            String secondKey = resolver.resolve(second);
            assertThat(firstKey).isEqualTo(secondKey).isEqualTo("biz-999");
        }
    }

    private static KafkaConsumer<String, String> newConsumer(EmbeddedKafkaBroker broker, String groupId) {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(groupId, "false", broker);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(consumerProps);
    }

    private static KafkaProducer<String, String> newProducer(EmbeddedKafkaBroker broker) {
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(broker);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaProducer<>(producerProps);
    }
}
