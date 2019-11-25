package net.qihoo.ads.flink.kafka.serde.thrift;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ThriftTBaseKafkaDeserializationSchema<T extends TBase> implements KafkaDeserializationSchema<T> {

    private Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final Class<T> thriftClazz;
    private final TProtocolFactory tProtocolFactory;

    public ThriftTBaseKafkaDeserializationSchema(Class<T> thriftClazz, TProtocolFactory tProtocolFactory) {
        this.thriftClazz = thriftClazz;
        this.tProtocolFactory = tProtocolFactory;
    }

    @Override
    public T deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        TDeserializer deserializer = new TDeserializer(tProtocolFactory);
        T t = null;
        try {
            t = thriftClazz.newInstance();
            deserializer.deserialize(t, consumerRecord.value());
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
        return t;
    }

    @Override
    public boolean isEndOfStream(T t) {
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(thriftClazz);
    }
}
