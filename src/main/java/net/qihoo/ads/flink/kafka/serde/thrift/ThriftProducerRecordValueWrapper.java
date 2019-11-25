package net.qihoo.ads.flink.kafka.serde.thrift;

import org.apache.thrift.TBase;

import java.io.Serializable;

/**
 * the generic wrapper for kafka ProducerRecord value wrapper for thrift
 */
public class ThriftProducerRecordValueWrapper implements Serializable {

  private String topic;

  private int partition;

  private long timestamp;

  private Class<? extends TBase> thriftClz; // the schema Class for thrift record

  private byte[] value; // the raw thrift record bytes

  public ThriftProducerRecordValueWrapper(String topic, int partition, long timestamp, Class<? extends TBase> thriftClz, byte[] value) {
    this.topic = topic;
    this.partition = partition;
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