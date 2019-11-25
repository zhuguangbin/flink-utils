package net.qihoo.ads.flink.kafka.serde.thrift;

import org.apache.thrift.TBase;

import java.io.Serializable;

/**
 * the generic wrapper for kafka ConsumerRecord value wrapper for thrift
 */
public class ThriftConsumerRecordValueWrapper implements Serializable {

  private String topic;

  private int partition;

  private long offset;

  private long timestamp;

  private Class<? extends TBase> thriftClz; // the schema Class for thrift record

  private byte[] value; // the raw thrift record bytes

  public ThriftConsumerRecordValueWrapper(String topic, int partition, long offset, long timestamp, Class<? extends TBase> thriftClz, byte[] value) {
    this.topic = topic;
    this.partition = partition;
    this.offset = offset;
    this.timestamp = timestamp;
    this.thriftClz = thriftClz;
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

  public Class<? extends TBase> getThriftClz() {
    return thriftClz;
  }

  public void setThriftClz(Class<? extends TBase> thriftClz) {
    this.thriftClz = thriftClz;
  }

  public byte[] getValue() {
    return value;
  }

  public void setValue(byte[] value) {
    this.value = value;
  }
}