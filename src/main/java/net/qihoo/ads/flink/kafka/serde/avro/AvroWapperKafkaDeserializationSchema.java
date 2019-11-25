package net.qihoo.ads.flink.kafka.serde.avro;

import org.apache.avro.Schema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroWapperKafkaDeserializationSchema implements KafkaDeserializationSchema<AvroConsumerRecordValueWrapper> {
    private Logger LOG = LoggerFactory.getLogger(this.getClass());

    private Schema schema;

    public AvroWapperKafkaDeserializationSchema(Schema schema) {
        this.schema = schema;
    }

    @Override
    public AvroConsumerRecordValueWrapper deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {

        // TODO: try to deserialize for verify

        return new AvroConsumerRecordValueWrapper(consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), consumerRecord.timestamp(), schema, consumerRecord.value());
    }


    @Override
    public boolean isEndOfStream(AvroConsumerRecordValueWrapper t) {
        return false;
    }

    @Override
    public TypeInformation<AvroConsumerRecordValueWrapper> getProducedType() {
        return new GenericTypeInfo<>(AvroConsumerRecordValueWrapper.class);
    }

}
