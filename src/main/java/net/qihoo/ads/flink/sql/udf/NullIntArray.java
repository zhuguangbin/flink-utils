package net.qihoo.ads.flink.sql.udf;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.functions.ScalarFunction;

public class NullIntArray extends ScalarFunction {

    public Integer[] eval() {
        return new Integer[]{};
    }

    @Override
    public TypeInformation getResultType(Class<?>[] signature) {
        return Types.OBJECT_ARRAY(Types.INT());
    }
}
