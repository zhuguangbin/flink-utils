package org.apache.flink.connector.jdbc.dialect;

import org.apache.flink.connector.jdbc.internal.converter.AerospikeRowConverter;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class AerospikeDialect extends AbstractDialect {

    private static final long serialVersionUID = 1L;

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:aerospike:");
    }

    @Override
    public JdbcRowConverter getRowConverter(RowType rowType) {
        return new AerospikeRowConverter(rowType);
    }

    @Override
    public String getLimitClause(long l) {
        return "LIMIT " + l;
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("com.aerospike.jdbc.AerospikeDriver");
    }

    @Override
    public String quoteIdentifier(String identifier) {
//        return "\"" + identifier + "\"";
        return identifier;
    }

    @Override
    public Optional<String> getUpsertStatement(
            String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        return Optional.of(
                getInsertIntoStatement(tableName, fieldNames));
    }

    @Override
    public String dialectName() {
        return "Aerospike";
    }

    @Override
    public int maxDecimalPrecision() {
        throw new RuntimeException("as not support decimal type");
    }

    @Override
    public int minDecimalPrecision() {
        throw new RuntimeException("as not support decimal type");
    }

    @Override
    public int maxTimestampPrecision() {
        throw new RuntimeException("as not support timestamp type");
    }

    @Override
    public int minTimestampPrecision() {
        throw new RuntimeException("as not support timestamp type");
    }

    @Override
    public List<LogicalTypeRoot> unsupportedTypes() {
        return Arrays.asList(
                LogicalTypeRoot.SMALLINT,
                LogicalTypeRoot.BINARY,
                LogicalTypeRoot.VARBINARY,
                LogicalTypeRoot.DECIMAL,
                LogicalTypeRoot.DATE,
                LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE,
                LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
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
                LogicalTypeRoot.UNRESOLVED);
    }
}
