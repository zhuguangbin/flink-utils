package net.qihoo.ads.flink.table.udf;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.stream.Stream;

public class StringToIntArray extends ScalarFunction {

    public Integer[] eval(String input) {
        if (input == null) {
            return null;
        }
        String[] items = input.split(",");
        if (items.length == 1) {
            return new Integer[]{};
        } else
            return ArrayUtils.toObject(Stream.of(items).mapToInt(Integer::parseInt).toArray());
    }

    @Override
    public TypeInformation getResultType(Class<?>[] signature) {
        return Types.OBJECT_ARRAY(Types.INT());
    }

}
