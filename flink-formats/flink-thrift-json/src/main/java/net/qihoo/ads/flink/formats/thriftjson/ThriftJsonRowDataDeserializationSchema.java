package net.qihoo.ads.flink.formats.thriftjson;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TSimpleJSONProtocol;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ThriftJsonRowDataDeserializationSchema extends JsonRowDataDeserializationSchema {
	private static final long serialVersionUID = 1L;
	private transient TDeserializer deserializer;
	private transient TSerializer serializer;

	private final String thriftClz;

	public ThriftJsonRowDataDeserializationSchema(RowType rowType, TypeInformation<RowData> resultTypeInfo,
	                                              boolean failOnMissingField,
	                                              boolean ignoreParseErrors,
	                                              TimestampFormat timestampFormat,
	                                              String thriftClz) {
		super(rowType, resultTypeInfo, failOnMissingField, ignoreParseErrors, timestampFormat);
		this.thriftClz = thriftClz;
	}

	@Override
	public void open(InitializationContext context) throws Exception {
		this.deserializer = new TDeserializer(new TBinaryProtocol.Factory());
		this.serializer = new TSerializer(new TSimpleJSONProtocol.Factory());
	}

	@Override
	public RowData deserialize(@Nullable byte[] message) throws IOException {
		try {
			TBase tBase = (TBase) Thread.currentThread().getContextClassLoader().loadClass(thriftClz).newInstance();
			this.deserializer.deserialize(tBase, message);
			String thriftJsonStr = this.serializer.toString(tBase);

			return super.deserialize(thriftJsonStr.getBytes(StandardCharsets.UTF_8));
		} catch (ClassNotFoundException e) {
			throw new IOException(String.format("Could not find thrift class: %s for deserialization, please check thrift-json format config options: " +
					"thrift-json.thrift-class, and put related jar in classpath ", thriftClz), e);
		} catch (TException | InstantiationException | IllegalAccessException e) {
			throw new IOException("Could not deserialize message for thrift-json format. ", e);
		}
	}

}
