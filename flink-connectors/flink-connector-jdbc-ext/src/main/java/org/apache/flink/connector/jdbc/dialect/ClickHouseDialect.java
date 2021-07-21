/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.dialect;

import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.internal.converter.MySQLRowConverter;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * JDBC dialect for MySQL.
 */
public class ClickHouseDialect extends AbstractDialect {

	private static final long serialVersionUID = 1L;

	// Define MAX/MIN precision of TIMESTAMP type according to Mysql docs:
	// https://dev.mysql.com/doc/refman/8.0/en/fractional-seconds.html
	private static final int MAX_TIMESTAMP_PRECISION = 6;
	private static final int MIN_TIMESTAMP_PRECISION = 1;

	// Define MAX/MIN precision of DECIMAL type according to Mysql docs:
	// https://dev.mysql.com/doc/refman/8.0/en/fixed-point-types.html
	private static final int MAX_DECIMAL_PRECISION = 65;
	private static final int MIN_DECIMAL_PRECISION = 1;

	@Override
	public boolean canHandle(String url) {
		return url.startsWith("jdbc:clickhouse:");
	}

	@Override
	public JdbcRowConverter getRowConverter(RowType rowType) {
		return new MySQLRowConverter(rowType);
	}

	// TODO: not implemented
	@Override
	public String getLimitClause(long l) {
		return null;
	}

	@Override
	public Optional<String> defaultDriverName() {
		return Optional.of("ru.yandex.clickhouse.ClickHouseDriver");
	}

	@Override
	public String quoteIdentifier(String identifier) {
		return "`" + identifier + "`";
	}

	@Override
	public Optional<String> getUpsertStatement(String tableName, String[] fieldNames, String[] uniqueKeyFields) {
		String columns = Arrays.stream(fieldNames)
				.collect(Collectors.joining(", "));
		String placeholders = Arrays.stream(fieldNames)
				.map(f -> quoteIdentifier(f))
				.collect(Collectors.joining(", "));
		return Optional.of(getInsertIntoStatement(tableName, fieldNames));
	}

	// TODO: not implemented
	@Override
	public String getRowExistsStatement(String tableName, String[] conditionFields) {
		return super.getRowExistsStatement(tableName, conditionFields);
	}

	// TODO: not implemented
	@Override
	public String getInsertIntoStatement(String tableName, String[] fieldNames) {
		return super.getInsertIntoStatement(tableName, fieldNames);
	}

	// TODO: not implemented
	@Override
	public String getUpdateStatement(String tableName, String[] fieldNames, String[] conditionFields) {
		return super.getUpdateStatement(tableName, fieldNames, conditionFields);
	}

	// TODO: not implemented
	@Override
	public String getDeleteStatement(String tableName, String[] conditionFields) {
		return super.getDeleteStatement(tableName, conditionFields);
	}

	// TODO: not implemented
	@Override
	public String getSelectFromStatement(String tableName, String[] selectFields, String[] conditionFields) {
		return super.getSelectFromStatement(tableName, selectFields, conditionFields);
	}

	@Override
	public String dialectName() {
		return "ClickHouse";
	}

	@Override
	public int maxDecimalPrecision() {
		return MAX_DECIMAL_PRECISION;
	}

	@Override
	public int minDecimalPrecision() {
		return MIN_DECIMAL_PRECISION;
	}

	@Override
	public int maxTimestampPrecision() {
		return MAX_TIMESTAMP_PRECISION;
	}

	@Override
	public int minTimestampPrecision() {
		return MIN_TIMESTAMP_PRECISION;
	}

	@Override
	public List<LogicalTypeRoot> unsupportedTypes() {
		// The data types used in Mysql are list at:
		// https://dev.mysql.com/doc/refman/8.0/en/data-types.html

		// TODO: We can't convert BINARY data type to
		//  PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO in LegacyTypeInfoDataTypeConverter.
		return Arrays.asList(
			LogicalTypeRoot.BINARY,
			LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
			LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE,
			LogicalTypeRoot.INTERVAL_YEAR_MONTH,
			LogicalTypeRoot.INTERVAL_DAY_TIME,
			LogicalTypeRoot.ARRAY,
			LogicalTypeRoot.MULTISET,
			LogicalTypeRoot.MAP,
			LogicalTypeRoot.ROW,
			LogicalTypeRoot.DISTINCT_TYPE,
			LogicalTypeRoot.STRUCTURED_TYPE,
			LogicalTypeRoot.NULL,
			LogicalTypeRoot.RAW,
			LogicalTypeRoot.SYMBOL,
			LogicalTypeRoot.UNRESOLVED
		);
	}
}
