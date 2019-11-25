package net.qihoo.ads.flink.kafka.serde.string;


import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * serialization schema for strings, with kafka meta.
 *
 * <p>By default, the serializer uses "UTF-8" for string/byte conversion.
 */
@PublicEvolving
public class StringKafkaSerializationSchema implements KafkaSerializationSchema<StringProducerRecordWrapper> {

    private static final long serialVersionUID = 1L;

    /**
     * The charset to use to convert between strings and bytes.
     * The field is transient because we serialize a different delegate object instead
     */
    private transient Charset charset;

    /**
     * Creates a new StringKafkaSerializationSchema that uses "UTF-8" as the encoding.
     */
    public StringKafkaSerializationSchema() {
        this(StandardCharsets.UTF_8);
    }

    /**
     * Creates a new StringKafkaSerializationSchema that uses the given charset to convert between strings and bytes.
     *
     * @param charset The charset to use to convert between strings and bytes.
     */
    public StringKafkaSerializationSchema(Charset charset) {
        this.charset = checkNotNull(charset);
    }

    /**
     * Gets the charset used by this schema for serialization.
     *
     * @return The charset used by this schema for serialization.
     */
    public Charset getCharset() {
        return charset;
    }

    // ------------------------------------------------------------------------
    //  Kafka Serialization
    // ------------------------------------------------------------------------

    @Override
    public ProducerRecord<byte[], byte[]> serialize(StringProducerRecordWrapper t, @Nullable Long timestamp) {
        return new ProducerRecord<>(t.getTopic(), t.getPartition(), t.getTimestamp(), t.getKey() == null ? null : t.getKey().getBytes(charset), t.getValue().getBytes(charset));
    }


    // ------------------------------------------------------------------------
    //  Java Serialization
    // ------------------------------------------------------------------------

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeUTF(charset.name());
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        String charsetName = in.readUTF();
        this.charset = Charset.forName(charsetName);
    }
}

