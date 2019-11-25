package net.qihoo.ads.flink.kafka.serde.avro;

import org.apache.avro.Schema;

import java.io.Serializable;

/**
 * the generic wrapper for kafka ConsumerRecord value wrapper for avro
 */
public class AvroProducerRecordValueWrapper implements Serializable {

  private String topic;

  private int partition;

  private long timestamp;

  private Schema schema; // the schema for avro record

  private byte[] value; // the raw avro record bytes

  public AvroProducerRecordValueWrapper(String topic, int partition, long timestamp, Schema schema, byte[] value) {
    this.topic = topic;
    this.partition = partition;
    this.timestamp = timestamp;
    this.schema = schema;
    this.value = value;
  }

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public int getPartition() {
    return partition;
  }

  public void setPartition(int partition) {
    this.partition = partition;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public Schema getSchema() {
    return schema;
  }

  public void setSchema(Schema schema) {
    this.schema = schema;
  }

  public byte[] getValue() {
    return value;
  }

  public void setValue(byte[] value) {
    this.value = value;
  }
}