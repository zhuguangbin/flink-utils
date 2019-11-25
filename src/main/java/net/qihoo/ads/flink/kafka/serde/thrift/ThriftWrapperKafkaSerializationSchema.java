package net.qihoo.ads.flink.kafka.serde.thrift;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TProtocolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public class ThriftWrapperKafkaSerializationSchema implements KafkaSerializationSchema<ThriftProducerRecordValueWrapper> {
    private Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final Class<? extends TBase> thriftClazz;
    private final TProtocolFactory tProtocolFactory;

    public ThriftWrapperKafkaSerializationSchema(Class<? extends TBase> thriftClazz, TProtocolFactory tProtocolFactory) {
        this.thriftClazz = thriftClazz;
        this.tProtocolFactory = tProtocolFactory;
    }


    @Override
    public ProducerRecord<byte[], byte[]> serialize(ThriftProducerRecordValueWrapper t, @Nullable Long timestamp) {
        return new ProducerRecord<>(t.getTopic(), t.getPartition(), t.getTimestamp(), null, t.getValue());
    }

}
