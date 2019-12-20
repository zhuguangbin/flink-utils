package net.qihoo.ads.flink.sql.udf;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;

public class TimestampToLong extends ScalarFunction {
    public Long eval(Timestamp input) {
        return input.getTime();
    }
}
