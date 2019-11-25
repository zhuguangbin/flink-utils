package net.qihoo.ads.flink.kafka.serde.thrift;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TProtocolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public class ThriftTBaseKafkaSerializationSchema<T extends TBase> implements KafkaSerializationSchema<T> {

    private Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final String topic;
    private final TProtocolFactory tProtocolFactory;

    public ThriftTBaseKafkaSerializationSchema(String topic, TProtocolFactory tProtocolFactory) {
        this.topic = topic;
        this.tProtocolFactory = tProtocolFactory;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(T t, @Nullable Long timestamp) {
        TSerializer serializer = new TSerializer(tProtocolFactory);
        byte[] serialized = new byte[0];
        try {
            serialized = serializer.serialize(t);
        } catch (TException e) {
            LOG.error("ERROR serializing {}", t.getClass(), e);
            e.printStackTrace();
        }
        return new ProducerRecord<>(topic, null, timestamp, null, serialized);
    }
}
