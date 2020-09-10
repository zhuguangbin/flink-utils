package net.qihoo.ads.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

import java.util.Collections;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Test split udtf. Registered under name 'mygenericudtf'
 */
public class TestHiveUDTF extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        checkArgument(argOIs.length == 2);

        // TEST for constant arguments
        checkArgument(argOIs[1] instanceof ConstantObjectInspector);
        Object constant = ((ConstantObjectInspector) argOIs[1]).getWritableConstantValue();
        checkArgument(constant instanceof IntWritable);
        checkArgument(((IntWritable) constant).get() == 1);

        return ObjectInspectorFactory.getStandardStructObjectInspector(
                Collections.singletonList("col1"),
                Collections.singletonList(PrimitiveObjectInspectorFactory.javaStringObjectInspector));
    }

    @Override
    public void process(Object[] args) throws HiveException {
        String str = (String) args[0];
        for (String s : str.split(",")) {
            forward(s);
            forward(s);
        }
    }

    @Override
    public void close() {
    }
}