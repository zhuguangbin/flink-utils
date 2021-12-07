package org.apache.flink.formats.jsonintsv;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatFactory;
import org.apache.flink.formats.json.JsonOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.formats.jsonintsv.JsonInTsvOptions.FIELD_DELIMITER;
import static org.apache.flink.formats.jsonintsv.JsonInTsvOptions.LINE_CHARSET;
import static org.apache.flink.formats.jsonintsv.JsonInTsvOptions.JSON_FIELD_INDEX;
import static org.apache.flink.formats.json.JsonOptions.ENCODE_DECIMAL_AS_PLAIN_NUMBER;
import static org.apache.flink.formats.json.JsonOptions.FAIL_ON_MISSING_FIELD;
import static org.apache.flink.formats.json.JsonOptions.IGNORE_PARSE_ERRORS;
import static org.apache.flink.formats.json.JsonOptions.MAP_NULL_KEY_LITERAL;
import static org.apache.flink.formats.json.JsonOptions.MAP_NULL_KEY_MODE;
import static org.apache.flink.formats.json.JsonOptions.TIMESTAMP_FORMAT;
import static org.apache.flink.formats.json.JsonOptions.validateDecodingFormatOptions;

public class JsonInTsvFormatFactory extends JsonFormatFactory {

	public static final String IDENTIFIER = "json-in-tsv";

	@Override
	public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(DynamicTableFactory.Context context, ReadableConfig formatOptions) {
		FactoryUtil.validateFactoryOptions(this, formatOptions);
		validateDecodingFormatOptions(formatOptions);

		final String lineCharset = formatOptions.get(LINE_CHARSET);
		final String fieldDelimiter = formatOptions.get(FIELD_DELIMITER);
		final Integer jsonFieldIndex = formatOptions.get(JSON_FIELD_INDEX);

		final boolean failOnMissingField = formatOptions.get(FAIL_ON_MISSING_FIELD);
		final boolean ignoreParseErrors = formatOptions.get(IGNORE_PARSE_ERRORS);
		TimestampFormat timestampOption = JsonOptions.getTimestampFormat(formatOptions);

		return new DecodingFormat<DeserializationSchema<RowData>>() {
			@Override
			public DeserializationSchema<RowData> createRuntimeDecoder(
					DynamicTableSource.Context context, DataType producedDataType) {
				final RowType rowType = (RowType) producedDataType.getLogicalType();
				final TypeInformation<RowData> rowDataTypeInfo =
						context.createTypeInformation(producedDataType);
				return new JsonInTsvRowDataDeserializationSchema(
						rowType,
						rowDataTypeInfo,
						failOnMissingField,
						ignoreParseErrors,
						timestampOption,
						lineCharset,
						fieldDelimiter,
						jsonFieldIndex);
			}

			@Override
			public ChangelogMode getChangelogMode() {
				return ChangelogMode.insertOnly();
			}
		};
	}

	@Override
	public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(DynamicTableFactory.Context context, ReadableConfig formatOptions) {
		throw new UnsupportedOperationException("json-in-tsv only used for source (deserialization)");
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		return Collections.singleton(JSON_FIELD_INDEX);
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> options = new HashSet<>();
		options.add(LINE_CHARSET);
		options.add(FIELD_DELIMITER);

		options.add(FAIL_ON_MISSING_FIELD);
		options.add(IGNORE_PARSE_ERRORS);
		options.add(TIMESTAMP_FORMAT);
		options.add(MAP_NULL_KEY_MODE);
		options.add(MAP_NULL_KEY_LITERAL);
		options.add(ENCODE_DECIMAL_AS_PLAIN_NUMBER);
		return options;
	}
}
