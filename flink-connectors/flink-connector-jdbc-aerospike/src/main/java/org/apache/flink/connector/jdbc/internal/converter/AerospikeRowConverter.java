package org.apache.flink.connector.jdbc.internal.converter;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.aerospike.client.Value;

import java.sql.ResultSet;
import java.sql.SQLException;

public class AerospikeRowConverter extends AbstractJdbcRowConverter {
    public AerospikeRowConverter(RowType rowType) {
        super(rowType);
    }

    @Override
    public RowData toInternal(ResultSet resultSet) throws SQLException {
        GenericRowData genericRowData = new GenericRowData(this.rowType.getFieldCount());
        for(int pos = 0; pos < this.rowType.getFieldCount(); ++pos) {
            Object field = resultSet.getObject(this.rowType.getFields().get(pos).getName());
            if (field == null) {
                genericRowData.setField(pos, null);
            } else {
                genericRowData.setField(pos, this.toInternalConverters[pos].deserialize(field));
            }
        }
        return genericRowData;
    }

    @Override
    protected JdbcDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return val -> null;
            case BOOLEAN:
                return val -> val instanceof Boolean ? val : ((Long) val).intValue() > 0;
            case FLOAT:
                return val -> {
                    try {
                        return ((Double) val).floatValue();
                    } catch (Exception e) {
                        throw e;
                    }
                };
            case DOUBLE:
                return val -> val instanceof Value.DoubleValue ? ((Value.DoubleValue) val).getObject() : val;
            case TINYINT:
                return val -> ((Long) val).byteValue();
            case INTEGER:
                return val -> ((Long) val).intValue();
            case BIGINT:
                return val -> val;
            case VARCHAR:
                return val -> StringData.fromString(val.toString());
            case SMALLINT:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
            case DECIMAL:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case CHAR:
            case BINARY:
            case VARBINARY:
            case ARRAY:
            case ROW:
            case MAP:
            case MULTISET:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    @Override
    public String converterName() {
        return "Aerospike";
    }
}
