package net.qihoo.ads.flink.formats.thriftjson.util;


import org.apache.thrift.TBase;
import org.apache.thrift.TEnum;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.EnumMetaData;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.ListMetaData;
import org.apache.thrift.meta_data.MapMetaData;
import org.apache.thrift.meta_data.SetMetaData;
import org.apache.thrift.meta_data.StructMetaData;
import org.apache.thrift.protocol.TType;

import java.util.Map;

public class FlinkTableSchemaGenerator {

	/**
	 * Generate Flink DDL for thrift-json format,
	 * given those three args: table-name, topic, thrift-class, kafka-bootstrap-servers
	 *
	 * @param args
	 * @throws NoSuchFieldException
	 */
	public static void main(String[] args) throws NoSuchFieldException, ClassNotFoundException, InstantiationException, IllegalAccessException {
		if (args.length != 4) {
			printUsage();
			System.exit(-1);
		} else {
			String tableName = args[0];
			String topic = args[1];
			String thriftClz = args[2];
			String kafkaBootstrapServers = args[3];

			System.out.println(generateFullDDL(tableName, topic, thriftClz, kafkaBootstrapServers));
		}

	}

	private static StringBuilder generateFullDDL(String tableName, String topic, String thriftClz, String kafkaBootstrapServers) throws NoSuchFieldException, ClassNotFoundException, InstantiationException, IllegalAccessException {
		StringBuilder sb = new StringBuilder();
		sb.append("CREATE TABLE " + unescape(tableName) + " (").append("\n");
		sb.append(generateSchema(thriftClz)).append("\n");
		sb.append(") WITH (").append("\n");
		sb.append("'connector'='kafka',").append("\n");
		sb.append("'topic'='" + topic + "',").append("\n");
		sb.append("'format'='thrift-json',").append("\n");
		sb.append("'thrift-json.thrift-class'='" + thriftClz + "',").append("\n");
		sb.append("'properties.bootstrap.servers'='" + kafkaBootstrapServers + "'").append("\n");
		sb.append(");");

		return sb;
	}

	private static StringBuilder generateSchema(String thriftClz) throws NoSuchFieldException, ClassNotFoundException, InstantiationException, IllegalAccessException {
		StringBuilder sb = new StringBuilder();
		TBase tBase = (TBase) Thread.currentThread().getContextClassLoader().loadClass(thriftClz).newInstance();
		Map<? extends TFieldIdEnum, FieldMetaData> metaDataMap = FieldMetaData.getStructMetaDataMap(tBase.getClass());
		for (TFieldIdEnum f : metaDataMap.keySet()) {
			sb.append(fieldSchema(f.getFieldName(), metaDataMap.get(f).valueMetaData)).append("\n");
		}
		sb.append(" procTime AS PROCTIME()");
		return sb;
	}

	private static void printUsage() {
		System.err.println("Usage: " + FlinkTableSchemaGenerator.class.getName() + ": <table-name> <topic> <thrift-class> <kafka-bootstrap-servers>");
	}

	/**
	 * Generate schema for each field
	 *
	 * @param fieldName
	 * @param fieldMetaData
	 * @return "fieldName FlinkType, "
	 * @throws NoSuchFieldException
	 */

	private static String fieldSchema(String fieldName, FieldValueMetaData fieldMetaData) throws NoSuchFieldException {

		switch (fieldMetaData.type) {
			case TType.BOOL:
				return unescape(fieldName) + " BOOLEAN, ";
			case TType.BYTE:
				return unescape(fieldName) + " TINYINT, ";
			case TType.DOUBLE:
				return unescape(fieldName) + " DOUBLE, ";
			case TType.I16:
				return unescape(fieldName) + " SMALLINT, ";
			case TType.I32:
				return unescape(fieldName) + " INTEGER, ";
			case TType.I64:
				return unescape(fieldName) + " BIGINT, ";
			case TType.STRING:
				if (fieldMetaData.isBinary()) {
					return unescape(fieldName) + " BYTES, ";
				} else {
					return unescape(fieldName) + " STRING, ";
				}
			case TType.STRUCT:
				StructMetaData structfield = (StructMetaData) fieldMetaData;
				return unescape(fieldName) + " " + structField2RowType(structfield.structClass) + ",";
			case TType.MAP:
				StringBuffer mapsb = new StringBuffer();
				mapsb.append(unescape(fieldName) + "MAP<");
				FieldValueMetaData key = ((MapMetaData) fieldMetaData).keyMetaData;
				FieldValueMetaData value = ((MapMetaData) fieldMetaData).valueMetaData;
				// JSON format doesn't support non-string as key type of map.
				//	mapsb.append(key.isStruct() ? structField2RowType(((StructMetaData) key).structClass) : nonStructType(key));
				mapsb.append("STRING");
				mapsb.append(" , ");
				mapsb.append(value.isStruct() ? structField2RowType(((StructMetaData) value).structClass) : nonStructType(value));
				mapsb.append(">, ");
				return mapsb.toString();
			case TType.SET:
				StringBuffer setsb = new StringBuffer();
				setsb.append(unescape(fieldName) + " MULTISET<");
				FieldValueMetaData setelem = ((SetMetaData) fieldMetaData).elemMetaData;
				setsb.append(setelem.isStruct() ? structField2RowType(((StructMetaData) setelem).structClass) : nonStructType(setelem));
				setsb.append(">, ");
				return setsb.toString();
			case TType.LIST:
				StringBuffer listsb = new StringBuffer();
				listsb.append(unescape(fieldName) + " ARRAY<");
				FieldValueMetaData listelem = ((ListMetaData) fieldMetaData).elemMetaData;
				listsb.append(listelem.isStruct() ? structField2RowType(((StructMetaData) listelem).structClass) : nonStructType(listelem));
				listsb.append(">, ");
				return listsb.toString();
			case TType.ENUM:
				Class<? extends TEnum> enumClass = ((EnumMetaData) fieldMetaData).enumClass;
				return unescape(fieldName) + " STRING, ";
			default:
				return null;
		}

	}

	/**
	 * Struct field type convert to Flink Row Type,
	 * it always return a Row
	 *
	 * @param clz
	 * @throws NoSuchFieldException
	 */

	private static String structField2RowType(Class clz) throws NoSuchFieldException {
		StringBuffer sb = new StringBuffer("Row<");

		Map<TFieldIdEnum, FieldMetaData> metaDataMap = FieldMetaData.getStructMetaDataMap(clz);
		for (TFieldIdEnum f : metaDataMap.keySet()) {
			switch (metaDataMap.get(f).valueMetaData.type) {

				case TType.BOOL:
					sb.append(unescape(f.getFieldName()) + " BOOLEAN, ");
					break;
				case TType.BYTE:
					sb.append(unescape(f.getFieldName()) + " TINYINT, ");
					break;
				case TType.DOUBLE:
					sb.append(unescape(f.getFieldName()) + " DOUBLE, ");
					break;
				case TType.I16:
					sb.append(unescape(f.getFieldName()) + " SMALLINT, ");
					break;
				case TType.I32:
					sb.append(unescape(f.getFieldName()) + " INTEGER, ");
					break;
				case TType.I64:
					sb.append(unescape(f.getFieldName()) + " BIGINT, ");
					break;
				case TType.STRING:
					if (metaDataMap.get(f).valueMetaData.isBinary()) {
						sb.append(unescape(f.getFieldName()) + " BYTES, ");
					} else {
						sb.append(unescape(f.getFieldName()) + " STRING, ");
					}
					break;
				case TType.STRUCT:
					sb.append(unescape(f.getFieldName()) + " ");
					sb.append(structField2RowType(clz.getField(f.getFieldName()).getType()));
					sb.append(", ");
					break;
				case TType.MAP:
					sb.append(unescape(f.getFieldName()) + " MAP<");
					FieldValueMetaData key = ((MapMetaData) metaDataMap.get(f).valueMetaData).keyMetaData;
					FieldValueMetaData value = ((MapMetaData) metaDataMap.get(f).valueMetaData).valueMetaData;
					// JSON format doesn't support non-string as key type of map.
					//	mapsb.append(key.isStruct() ? structField2RowType(((StructMetaData) key).structClass) : nonStructType(key));
					sb.append("STRING");
					sb.append(" , ");
					sb.append(value.isStruct() ? structField2RowType(((StructMetaData) value).structClass) : nonStructType(value));
					sb.append(">, ");
					break;
				case TType.SET:
					sb.append(unescape(f.getFieldName()) + " MULTISET<");
					FieldValueMetaData setelem = ((SetMetaData) metaDataMap.get(f).valueMetaData).elemMetaData;
					sb.append(setelem.isStruct() ? structField2RowType(((StructMetaData) setelem).structClass) : nonStructType(setelem));
					sb.append(">, ");
					break;
				case TType.LIST:
					sb.append(f.getFieldName() + " ARRAY<");
					FieldValueMetaData listelem = ((ListMetaData) metaDataMap.get(f).valueMetaData).elemMetaData;
					sb.append(listelem.isStruct() ? structField2RowType(((StructMetaData) listelem).structClass) : nonStructType(listelem));
					sb.append(">, ");
					break;
				case TType.ENUM:
					Class<? extends TEnum> enumClass = ((EnumMetaData) metaDataMap.get(f).valueMetaData).enumClass;
					sb.append(unescape(f.getFieldName()) + " STRING, ");
					break;
			}
		}

		// delete last comma
		sb.deleteCharAt(sb.lastIndexOf(","));

		sb.append(">");
		return sb.toString();

	}

	/**
	 * non Struct Type convert to elementry type
	 *
	 * @param f
	 * @return
	 * @throws NoSuchFieldException
	 */

	private static String nonStructType(FieldValueMetaData f) throws NoSuchFieldException {
		switch (f.type) {
			case TType.BOOL:
				return "BOOLEAN";
			case TType.BYTE:
				return "TINYINT";
			case TType.DOUBLE:
				return "DOUBLE";
			case TType.I16:
				return "SMALLINT";
			case TType.I32:
				return "INTEGER";
			case TType.I64:
				return "BIGINT";
			case TType.STRING:
				if (f.isBinary()) {
					return "BYTES";
				} else {
					return "STRING";
				}
			case TType.ENUM:
				return "STRING";
			case TType.MAP:
				return "MAP<" + nonStructType(((MapMetaData) f).keyMetaData) + ": " + nonStructType(((MapMetaData) f).valueMetaData) + ">";
			case TType.SET:
				return "MULTISET<" + nonStructType(((SetMetaData) f).elemMetaData) + ">";
			case TType.LIST:
				return "ARRAY<" + nonStructType(((ListMetaData) f).elemMetaData) + ">";
			case TType.STRUCT:
				throw new IllegalArgumentException("invalid type, here is only for non struct type, but is " + f.type);
		}

		return null;
	}

	private static String unescape(String fieldName) {
		return "`" + fieldName + "`";
	}


}
