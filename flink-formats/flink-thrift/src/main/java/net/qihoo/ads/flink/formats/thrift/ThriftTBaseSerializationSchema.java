package net.qihoo.ads.flink.formats.thrift;

import org.apache.flink.api.common.serialization.SerializationSchema;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TProtocolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftTBaseSerializationSchema<T extends TBase> implements SerializationSchema<T> {
    private Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final TProtocolFactory tProtocolFactory;

    public ThriftTBaseSerializationSchema(TProtocolFactory tProtocolFactory) {
        this.tProtocolFactory = tProtocolFactory;
    }

    @Override
    public byte[] serialize(T object) {
        TSerializer serializer = new TSerializer(tProtocolFactory);
        byte[] serialized = new byte[0];
        try {
            serialized = serializer.serialize(object);
        } catch (TException e) {
            LOG.error("Failed to serialize {}", object.getClass(), e);
            e.printStackTrace();
        }
        return serialized;
    }
}
