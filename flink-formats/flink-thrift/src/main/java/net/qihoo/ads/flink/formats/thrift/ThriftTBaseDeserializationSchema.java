package net.qihoo.ads.flink.formats.thrift;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ThriftTBaseDeserializationSchema<T extends TBase> implements DeserializationSchema<T> {
    private Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final Class<T> thriftClazz;
    private final TProtocolFactory tProtocolFactory;

    public ThriftTBaseDeserializationSchema(Class<T> thriftClazz, TProtocolFactory tProtocolFactory) {
        this.thriftClazz = thriftClazz;
        this.tProtocolFactory = tProtocolFactory;
    }

    @Override
    public T deserialize(byte[] message) throws IOException {
        TDeserializer deserializer = new TDeserializer(tProtocolFactory);
        T t = null;
        try {
            t = thriftClazz.newInstance();
            deserializer.deserialize(t, message);
        } catch (InstantiationException e) {
            LOG.error("Failed to deserialize to {}", thriftClazz.toString(), e);
            throw new IOException(e);
        } catch (IllegalAccessException e) {
            LOG.error("Failed to deserialize to {}", thriftClazz.toString(), e);
            throw new IOException(e);
        } catch (TException e) {
            LOG.error("Failed to deserialize to {}", thriftClazz.toString(), e);
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
