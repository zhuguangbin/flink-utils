package net.qihoo.ads.flink.kafka.serde.avro;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericContainer;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.util.Preconditions;

import java.util.Map;
import java.util.Properties;

public class ConfluentRegistryAvroSerializationSchema<T extends GenericContainer> implements SerializationSchema<T> {

    private static final int DEFAULT_IDENTITY_MAP_CAPACITY = 1000;

    private static final long serialVersionUID = 1L;

    private final String topic;
    private final String url;
    private Properties props;
    private int identityMapCapacity = DEFAULT_IDENTITY_MAP_CAPACITY;

    private transient KafkaAvroSerializer kafkaAvroSerializer;

    public ConfluentRegistryAvroSerializationSchema(String topic, String url) {
        this.topic = topic;
        this.url = url;
    }

    public ConfluentRegistryAvroSerializationSchema(String topic, String url, Properties props) {
        this.topic = topic;
        this.url = url;
        this.props = props;
    }

    public ConfluentRegistryAvroSerializationSchema(String topic, String url, Properties props, int identityMapCapacity) {
        this.topic = topic;
        this.url = url;
        this.props = props;
        this.identityMapCapacity = identityMapCapacity;
    }

    @Override
    public byte[] serialize(T elem) {
        checkAvroInitialized();
        return kafkaAvroSerializer.serialize(topic, elem);
    }

    private void checkAvroInitialized() {
        if (kafkaAvroSerializer == null) {
            Preconditions.checkNotNull(topic);
            Preconditions.checkNotNull(url);
            if (props == null) {
                this.kafkaAvroSerializer = new KafkaAvroSerializer(new CachedSchemaRegistryClient(url, identityMapCapacity));
            } else {
                this.kafkaAvroSerializer = new KafkaAvroSerializer(new CachedSchemaRegistryClient(url, identityMapCapacity), (Map)props);
            }
        }
    }

}
