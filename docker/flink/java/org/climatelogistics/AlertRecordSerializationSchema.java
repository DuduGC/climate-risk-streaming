package org.climatelogistics;

import java.nio.charset.StandardCharsets;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema.KafkaSinkContext;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.flink.types.Row;

/** Serializes a two-column alert row as the Kafka key and value. */
public final class AlertRecordSerializationSchema
        implements KafkaRecordSerializationSchema<Row> {
    private final String topic;

    public AlertRecordSerializationSchema(String topic) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic must be non-empty");
        }
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            Row row, KafkaSinkContext context, Long timestamp) {
        if (row == null || row.getArity() != 2) {
            throw new IllegalArgumentException("alert row must contain exactly two fields");
        }
        Object locationId = row.getField(0);
        Object payload = row.getField(1);
        if (!(locationId instanceof String) || !(payload instanceof String)) {
            throw new IllegalArgumentException("alert row fields must be non-null strings");
        }

        return new ProducerRecord<>(
                topic,
                ((String) locationId).getBytes(StandardCharsets.UTF_8),
                ((String) payload).getBytes(StandardCharsets.UTF_8));
    }
}
