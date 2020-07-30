package org.apache.flink.formats.thrift.elephantbird;

import org.apache.flink.formats.thrift.ThriftBuilder;
import org.apache.flink.formats.thrift.ThriftWriterFactory;

import com.hadoop.compression.lzo.LzopCodec;
import com.twitter.elephantbird.mapreduce.io.RawBlockWriter;
import com.twitter.elephantbird.mapreduce.io.ThriftBlockWriter;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.thrift.TBase;

import java.io.IOException;
import java.io.OutputStream;

public class ElephantBirdThriftWriters {

    public static <T extends TBase<?, ?>> ThriftWriterFactory<T> forLzoThriftBlock(Class<T> type) {

        final ThriftBuilder<T> builder = (out) -> createLzoThriftBlockWriter(type, out);
        return new ThriftWriterFactory<>(builder);
    }

    public static ThriftWriterFactory<byte[]> forLzoRawBlock() {
        final ThriftBuilder<byte[]> builder = (out) -> createLzoRawBlockWriter(out);
        return new ThriftWriterFactory<byte[]>(builder);
    }

    private static <T> ThriftBlockWriter createLzoThriftBlockWriter(
            Class<T> type,
            OutputStream out) throws IOException {
        LzopCodec codec = new LzopCodec();
        CompressionOutputStream compressOut = codec.createOutputStream(out);
        return new ThriftBlockWriter(compressOut, type);
    }

    private static RawBlockWriter createLzoRawBlockWriter(OutputStream out) throws IOException {
        LzopCodec codec = new LzopCodec();
        CompressionOutputStream compressOut = codec.createOutputStream(out);
        return new RawBlockWriter(compressOut);
    }
}
