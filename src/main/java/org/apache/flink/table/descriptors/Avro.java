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

package org.apache.flink.table.descriptors;

import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import java.util.Map;

/**
 * Format descriptor for Apache Avro records.
 */
@PublicEvolving
public class Avro extends FormatDescriptor {

	private boolean useRegistry;
	private String registryUrl;
	private String registrySubject;
	private Class<? extends SpecificRecord> recordClass;
	private String avroSchema;

	/**
	 * Format descriptor for Apache Avro records.
	 */
	public Avro() {
		super(AvroValidator.FORMAT_TYPE_VALUE, 2);
	}

	/**
	 * Set whether use schema registry.
	 *
	 * @param useRegistry whether useRegistry or not.
	 */
	public Avro useRegistry(boolean useRegistry) {
		Preconditions.checkNotNull(useRegistry);
		this.useRegistry = useRegistry;
		return this;
	}

	/**
	 * Set schema registry url.
	 *
	 * @param registryUrl schema registry url.
	 */
	public Avro registryUrl(String registryUrl) {
		Preconditions.checkNotNull(registryUrl);
		this.registryUrl = registryUrl;
		return this;
	}

	/**
	 * Registry subject of schema
	 * @param registrySubject subject name of schema
	 * @return
	 */
	public Avro registrySubject(String registrySubject) {
		Preconditions.checkNotNull(registrySubject);
		this.registrySubject = registrySubject;
		return this;
	}

	/**
	 * Sets the class of the Avro specific record.
	 *
	 * @param recordClass class of the Avro record.
	 */
	public Avro recordClass(Class<? extends SpecificRecord> recordClass) {
		Preconditions.checkNotNull(recordClass);
		this.recordClass = recordClass;
		return this;
	}

	/**
	 * Sets the Avro schema for specific or generic Avro records.
	 *
	 * @param avroSchema Avro schema string
	 */
	public Avro avroSchema(String avroSchema) {
		Preconditions.checkNotNull(avroSchema);
		this.avroSchema = avroSchema;
		return this;
	}

	@Override
	protected Map<String, String> toFormatProperties() {
		final DescriptorProperties properties = new DescriptorProperties();

		properties.putBoolean(AvroValidator.FORMAT_USE_REGISTRY, useRegistry);

		if (null != registryUrl) {
			properties.putString(AvroValidator.FORMAT_REGISTRY_URL, registryUrl);
		}
		if (null != registrySubject) {
			properties.putString(AvroValidator.FORMAT_REGISTRY_SUBJECT, registrySubject);
		}
		if (null != recordClass) {
			properties.putClass(AvroValidator.FORMAT_RECORD_CLASS, recordClass);
		}
		if (null != avroSchema) {
			properties.putString(AvroValidator.FORMAT_AVRO_SCHEMA, avroSchema);
		}

		return properties.asMap();
	}
}
