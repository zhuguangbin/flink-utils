package net.qihoo.ads.flink.kafka.serde.thrift;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ThriftWapperKafkaDeserializationSchema implements KafkaDeserializationSchema<ThriftConsumerRecordValueWrapper> {
    private Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final Class<? extends TBase> thriftClazz;
    private final TProtocolFactory tProtocolFactory;
    private boolean verify = false;

    public ThriftWapperKafkaDeserializationSchema(Class<? extends TBase> thriftClazz, TProtocolFactory tProtocolFactory) {
        this.thriftClazz = thriftClazz;
        this.tProtocolFactory = tProtocolFactory;
    }

    public ThriftWapperKafkaDeserializationSchema(Class<? extends TBase> thriftClazz, TProtocolFactory tProtocolFactory, Boolean verify) {
        this.thriftClazz = thriftClazz;
        this.tProtocolFactory = tProtocolFactory;
        this.verify = verify;
    }


    @Override
    public ThriftConsumerRecordValueWrapper deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        if (verify) {
            tryDeserialize(consumerRecord.value());
        }

        return new ThriftConsumerRecordValueWrapper(consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), consumerRecord.timestamp(), thriftClazz, consumerRecord.value());
    }


    @Override
    public boolean isEndOfStream(ThriftConsumerRecordValueWrapper t) {
        return false;
    }

    @Override
    public TypeInformation<ThriftConsumerRecordValueWrapper> getProducedType() {
        return new GenericTypeInfo<>(ThriftConsumerRecordValueWrapper.class);
    }

    /*
        try to deserialize , to check it is the right thrift record
     */
    private void tryDeserialize(byte[] bytes) throws IOException {

        TDeserializer deserializer = new TDeserializer(tProtocolFactory);
        TBase t = null;
        // just verify thrift schema
        try {
            t = thriftClazz.newInstance();
            deserializer.deserialize(t, bytes);
        } catch (InstantiationException e) {
            LOG.error("ERROR deserialize to {}", thriftClazz.toString(), e);
            throw new IOException(e);
        } catch (IllegalAccessException e) {
            LOG.error("ERROR deserialize to {}", thriftClazz.toString(), e);
            throw new IOException(e);
        } catch (TException e) {
            LOG.error("ERROR deserialize to {}", thriftClazz.toString(), e);
            throw new IOException(e);
        }
    }

}
