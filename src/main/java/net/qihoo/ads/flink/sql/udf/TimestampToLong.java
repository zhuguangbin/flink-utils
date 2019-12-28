package net.qihoo.ads.flink.sql.udf;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;
import java.util.TimeZone;

public class TimestampToLong extends ScalarFunction {
    private static final TimeZone LOCAL_TZ = TimeZone.getDefault();

    public Long eval(Timestamp ts) {
        long time = ts.getTime();
        return time + LOCAL_TZ.getOffset(time);
    }
}
