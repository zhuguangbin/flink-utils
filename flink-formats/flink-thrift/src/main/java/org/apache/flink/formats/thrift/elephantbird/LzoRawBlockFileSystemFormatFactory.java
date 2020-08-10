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

package org.apache.flink.formats.thrift.elephantbird;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.formats.thrift.ThriftBulkWriter;
import org.apache.flink.formats.thrift.ThriftWriterFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FileSystemFormatFactory;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Elephantbird LzoThriftBlock format factory for file system.
 */
public class LzoRawBlockFileSystemFormatFactory implements FileSystemFormatFactory {

    public static final String IDENTIFIER = "lzoRawBlock";

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

    @Override
    public InputFormat<RowData, ?> createReader(ReaderContext context) {
        throw new UnsupportedOperationException("not support reading lzoThriftBlock");
    }

    @Override
    public Optional<Encoder<RowData>> createEncoder(WriterContext context) {
        return Optional.empty();
    }

    @Override
    public Optional<BulkWriter.Factory<RowData>> createBulkWriterFactory(WriterContext context) {
        return Optional.of(new RowDataRawBlockWriterFactory(context.getFormatRowType()));
    }

    /**
     * A {@link BulkWriter.Factory} to convert {@link RowData} to bytes[] and
     * wrap {@link ThriftWriterFactory}.
     */
    private static class RowDataRawBlockWriterFactory implements BulkWriter.Factory<RowData> {

        private static final long serialVersionUID = 1L;

        private final ThriftWriterFactory factory;
        private final RowType rowType;

        private RowDataRawBlockWriterFactory(RowType rowType) {
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
                            && rowType.getFields().get(0).getName().equals("rawvalue")
                            && rowType.getFields().get(0).getType().getTypeRoot() == LogicalTypeRoot.VARBINARY
                            && element.getArity() == 1
                    ) {
                        byte[] rawbytes = element.getBinary(0);
                        writer.addElement(rawbytes);
                    } else {
                        throw new IOException("incorrect format, not lzoThriftBlock format ");
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
