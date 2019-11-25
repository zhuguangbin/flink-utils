package net.qihoo.ads.flink.kafka.serde.avro;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public class AvroWrapperKafkaSerializationSchema implements KafkaSerializationSchema<AvroProducerRecordValueWrapper> {
    private Logger LOG = LoggerFactory.getLogger(this.getClass());


    @Override
    public ProducerRecord<byte[], byte[]> serialize(AvroProducerRecordValueWrapper t, @Nullable Long timestamp) {

        // TODO: try to serialize for verify

        return new ProducerRecord<>(t.getTopic(), t.getPartition(), t.getTimestamp(), null, t.getValue());
    }

}
