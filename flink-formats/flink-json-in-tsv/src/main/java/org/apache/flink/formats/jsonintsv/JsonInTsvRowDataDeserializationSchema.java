package org.apache.flink.formats.jsonintsv;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.io.IOException;

public class JsonInTsvRowDataDeserializationSchema extends JsonRowDataDeserializationSchema {
	private static final long serialVersionUID = 1L;

	private final String lineCharset;
	private final String fieldDelimiter;
	private final Integer jsonFieldIndex;

	public JsonInTsvRowDataDeserializationSchema(RowType rowType, TypeInformation<RowData> resultTypeInfo,
	                                             boolean failOnMissingField,
	                                             boolean ignoreParseErrors,
	                                             TimestampFormat timestampFormat,
												 String lineCharset,
	                                             String fieldDelimiter,
	                                             Integer jsonFieldIndex) {
		super(rowType, resultTypeInfo, failOnMissingField, ignoreParseErrors, timestampFormat);
		this.lineCharset = lineCharset;
		this.fieldDelimiter = fieldDelimiter;
		this.jsonFieldIndex = jsonFieldIndex;
	}

	@Override
	public RowData deserialize(@Nullable byte[] message) throws IOException {
		try {
			String msgStr = new String(message, lineCharset);
			String jsonStr = msgStr.split(fieldDelimiter)[jsonFieldIndex];
			return super.deserialize(jsonStr.getBytes(lineCharset));
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new IOException(String.format("error parsing message, please check json-in-tsv format config options: " +
					"field-delimiter: %s, json-field-index: %d ", fieldDelimiter, jsonFieldIndex), e);
		}
	}

}
