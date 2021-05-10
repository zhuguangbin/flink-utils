package org.apache.flink.formats.thrift.elephantbird;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.formats.thrift.ThriftBulkWriter;
import org.apache.flink.formats.thrift.ThriftWriterFactory;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.BulkWriterFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class LzoRawBlockFileSystemFormatFactory implements BulkWriterFormatFactory {

	public static final String IDENTIFIER = "lzoRawBlock";

	@Override
	public EncodingFormat<BulkWriter.Factory<RowData>> createEncodingFormat(DynamicTableFactory.Context context, ReadableConfig formatOptions) {
		return new EncodingFormat<BulkWriter.Factory<RowData>>() {
			@Override
			public BulkWriter.Factory<RowData> createRuntimeEncoder(
					DynamicTableSink.Context sinkContext, DataType consumedDataType) {
				return new RowDataLzoRawBlockWriterFactory(
						(RowType) consumedDataType.getLogicalType());
			}

			@Override
			public ChangelogMode getChangelogMode() {
				return ChangelogMode.insertOnly();
			}
		};
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		return new HashSet<>();
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		return new HashSet<>();
	}


	/**
	 * A {@link BulkWriter.Factory} to convert {@link RowData} to bytes[] and
	 * wrap {@link ThriftWriterFactory}.
	 */
	private static class RowDataLzoRawBlockWriterFactory implements BulkWriter.Factory<RowData> {

		private static final long serialVersionUID = 1L;

		private final ThriftWriterFactory factory;
		private final RowType rowType;

		private RowDataLzoRawBlockWriterFactory(RowType rowType) {
			this.rowType = rowType;
			this.factory = ElephantBirdThriftWriters.forLzoRawBlock();
		}

		@Override
		public BulkWriter<RowData> create(FSDataOutputStream out) throws IOException {
			ThriftBulkWriter writer = (ThriftBulkWriter) factory.create(out);
			return new BulkWriter<RowData>() {

				@Override
				public void addElement(RowData element) throws IOException {

					if (rowType.getFieldCount() == 1
							&& rowType.getFields().get(0).getType().getTypeRoot() == LogicalTypeRoot.VARBINARY
							&& element.getArity() == 1
					) {
						byte[] rawbytes = element.getBinary(0);
						writer.addElement(rawbytes);
					} else {
						throw new IOException("incorrect format, not lzoThriftBlock format, required only one BYTES/VARBINARY field");
					}
				}

				@Override
				public void flush() throws IOException {
					writer.flush();
				}

				@Override
				public void finish() throws IOException {
					writer.finish();
				}
			};
		}
	}
}
