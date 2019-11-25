package net.qihoo.ads.flink.kafka.serde.string;

import java.io.Serializable;

/**
 * the generic kafka ProducerRecord wrapper for String
 */
public class StringProducerRecordWrapper implements Serializable {

  private String topic;

  private int partition;

  private long timestamp;

  private String key; // the raw string key

  private String value; // the raw string value

  public StringProducerRecordWrapper(String topic, int partition, long timestamp, String key, String value) {
    this.topic = topic;
    this.partition = partition;
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