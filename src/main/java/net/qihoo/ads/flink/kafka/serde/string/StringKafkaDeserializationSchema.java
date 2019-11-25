package net.qihoo.ads.flink.kafka.serde.string;


import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 *  deserialization schema for strings, with kafka meta.
 *
 * <p>By default, the serializer uses "UTF-8" for string/byte conversion.
 */
@PublicEvolving
public class StringKafkaDeserializationSchema implements KafkaDeserializationSchema<StringConsumerRecordWrapper> {

    private static final long serialVersionUID = 1L;

    /** The charset to use to convert between strings and bytes.
     * The field is transient because we serialize a different delegate object instead */
    private transient Charset charset;

    /**
     * Creates a new StringKafkaDeserializationSchema that uses "UTF-8" as the encoding.
     */
    public StringKafkaDeserializationSchema() {
        this(StandardCharsets.UTF_8);
    }

    /**
     * Creates a new StringKafkaDeserializationSchema that uses the given charset to convert between strings and bytes.
     *
     * @param charset The charset to use to convert between strings and bytes.
     */
    public StringKafkaDeserializationSchema(Charset charset) {
        this.charset = checkNotNull(charset);
    }

    /**
     * Gets the charset used by this schema for serialization.
     * @return The charset used by this schema for serialization.
     */
    public Charset getCharset() {
        return charset;
    }

    // ------------------------------------------------------------------------
    //  Kafka Deserialization
    // ------------------------------------------------------------------------

    @Override
    public StringConsumerRecordWrapper deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        return new StringConsumerRecordWrapper(consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), consumerRecord.timestamp(), consumerRecord.key() == null? null : new String(consumerRecord.key(), charset),  new String(consumerRecord.value(), charset));
    }


    @Override
    public boolean isEndOfStream(StringConsumerRecordWrapper nextElement) {
        return false;
    }

    @Override
    public TypeInformation<StringConsumerRecordWrapper> getProducedType() {
        return new GenericTypeInfo<>(StringConsumerRecordWrapper.class);
    }

    // ------------------------------------------------------------------------
    //  Java Serialization
    // ------------------------------------------------------------------------

    private void writeObject (ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeUTF(charset.name());
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        String charsetName = in.readUTF();
        this.charset = Charset.forName(charsetName);
    }
}

