package net.qihoo.ads.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * Test simple udf. Registered under name 'myudf'
 */
public class TestHiveSimpleUDF extends UDF {

    public IntWritable evaluate(IntWritable i) {
        return new IntWritable(i.get());
    }

    public Text evaluate(Text text) {
        return new Text(text.toString());
    }
}
