package net.qihoo.ads.flink.table.udf;

import org.apache.flink.table.functions.ScalarFunction;

public class NullableIntToString extends ScalarFunction {

    public String eval(Integer input) {
        return String.valueOf(input);
    }

}
