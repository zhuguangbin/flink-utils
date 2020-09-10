package net.qihoo.ads.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.IntWritable;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Test generic udf. Registered under name 'mygenericudf'
 */
public class TestHiveGenericUDF extends GenericUDF {

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        checkArgument(arguments.length == 2);

        checkArgument(arguments[1] instanceof ConstantObjectInspector);
        Object constant = ((ConstantObjectInspector) arguments[1]).getWritableConstantValue();
        checkArgument(constant instanceof IntWritable);
        checkArgument(((IntWritable) constant).get() == 1);

        if (arguments[0] instanceof IntObjectInspector ||
                arguments[0] instanceof StringObjectInspector) {
            return arguments[0];
        } else {
            throw new RuntimeException("Not support argument: " + arguments[0]);
        }
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        return arguments[0].get();
    }

    @Override
    public String getDisplayString(String[] children) {
        return "TestHiveGenericUDF";
    }
}
