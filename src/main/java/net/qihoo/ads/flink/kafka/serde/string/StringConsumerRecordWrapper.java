package net.qihoo.ads.flink.kafka.serde.string;

import org.apache.thrift.TBase;

import java.io.Serializable;

/**
 * the generic kafka ConsumerRecord wrapper for String
 */
public class StringConsumerRecordWrapper implements Serializable {

  private String topic;

  private int partition;

  private long offset;

  private long timestamp;

  private String key; // the raw string key

  private String value; // the raw string value

  public StringConsumerRecordWrapper(String topic, int partition, long offset, long timestamp, String key, String value) {
    this.topic = topic;
    this.partition = partition;
    this.offset = offset;
    this.timestamp = timestamp;
    this.key = key;
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

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }
}