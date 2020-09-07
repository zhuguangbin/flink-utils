package org.apache.flink.formats.raw;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;

public class RawToRowDataConverters {

    /**
     * Runtime converter that converts raw bytes into objects of Flink Table & SQL
     * internal data structures.
     */
    @FunctionalInterface
    public interface RawToRowDataConverter extends Serializable {
        Object convert(byte[] object);
    }

    // -------------------------------------------------------------------------------------
    // Runtime Converters
    // -------------------------------------------------------------------------------------

    public static RawToRowDataConverter createRowConverter(RowType rowType) {
        final int arity = rowType.getFieldCount();
        assert arity == 1;
        assert rowType.getFields().get(0).getType().getTypeRoot() == LogicalTypeRoot.VARBINARY;

        return rawObject -> {
            GenericRowData row = new GenericRowData(arity);
            row.setField(0, rawObject);
            return row;
        };
    }
}
