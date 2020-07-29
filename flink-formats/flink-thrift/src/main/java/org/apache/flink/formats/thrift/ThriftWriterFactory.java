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

package org.apache.flink.formats.thrift;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;

import java.io.IOException;

/**
 * A factory that creates a Thrift {@link BulkWriter}. The factory takes a user-supplied
 * builder to assemble Thrift's writer and then turns it into a Flink {@code BulkWriter}.
 *
 * @param <T> The type of record to write.
 */
@PublicEvolving
public class ThriftWriterFactory<T> implements BulkWriter.Factory<T> {

	private static final long serialVersionUID = 1L;

	/** The builder to construct the ThriftWriter. */
	private final ThriftBuilder<T> writerBuilder;

	/**
	 * Creates a new ThriftWriterFactory using the given builder to assemble the
	 * ThriftWriter.
	 *
	 * @param writerBuilder The builder to construct the ThriftWriter.
	 */
	public ThriftWriterFactory(ThriftBuilder<T> writerBuilder) {
		this.writerBuilder = writerBuilder;
	}

	@Override
	public BulkWriter<T> create(FSDataOutputStream stream) throws IOException {
		return new ThriftBulkWriter<T>(writerBuilder.createWriter(stream));
	}
}
