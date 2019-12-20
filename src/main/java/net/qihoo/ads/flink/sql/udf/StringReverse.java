package net.qihoo.ads.flink.sql.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.functions.ScalarFunction;

public class StringReverse extends ScalarFunction {

    public String eval(String input) {
        return StringUtils.reverse(input);
    }

}
