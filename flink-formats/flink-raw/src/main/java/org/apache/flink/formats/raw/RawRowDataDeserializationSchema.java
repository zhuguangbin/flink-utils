package org.apache.flink.formats.raw;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;

public class RawRowDataDeserializationSchema implements DeserializationSchema<RowData> {

    /**
     * Type information describing the result type.
     */
    private final TypeInformation<RowData> typeInfo;

    private final RawToRowDataConverters.RawToRowDataConverter runtimeConverter;

    public RawRowDataDeserializationSchema(RowType rowType, TypeInformation<RowData> typeInfo)  {
        this.typeInfo = typeInfo;
        this.runtimeConverter = RawToRowDataConverters.createRowConverter(rowType);
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        return (RowData) runtimeConverter.convert(message);
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return typeInfo;
    }
}
