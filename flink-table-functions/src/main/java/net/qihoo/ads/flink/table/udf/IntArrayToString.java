package net.qihoo.ads.flink.table.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.functions.ScalarFunction;


public class IntArrayToString extends ScalarFunction {

    public String eval(Integer[] input) {
        return StringUtils.join(input, ",");
    }

}
