package net.qihoo.ads.flink.sink.filesystem.bucketassigners;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

@PublicEvolving
public class HiveDateHourPartitionBucketAssigner<IN> implements BucketAssigner<IN, String> {

    private static final long serialVersionUID = 1L;

    public HiveDateHourPartitionBucketAssigner() {
    }

    @Override
    public String getBucketId(IN element, Context context) {
        return "date=" + DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.systemDefault()).format(Instant.ofEpochMilli(context.timestamp()))
                + "/hour="+ DateTimeFormatter.ofPattern("HH").withZone(ZoneId.systemDefault()).format(Instant.ofEpochMilli(context.timestamp()));
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }

}

