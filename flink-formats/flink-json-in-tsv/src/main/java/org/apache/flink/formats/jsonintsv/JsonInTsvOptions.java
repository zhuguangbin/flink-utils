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

package org.apache.flink.formats.jsonintsv;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** This class holds configuration constants used by json format. */
public class JsonInTsvOptions {

    public static final ConfigOption<String> FIELD_DELIMITER =
            ConfigOptions.key("field-delimiter")
                    .stringType()
                    .defaultValue("\t")
                    .withDescription("Optional field delimiter character ('\t' by default)");

    public static final ConfigOption<String> LINE_CHARSET =
            ConfigOptions.key("line-charset")
                    .stringType()
                    .defaultValue("UTF-8")
                    .withDescription("Optional charset of line ('UTF-8' by default)");

    public static final ConfigOption<Integer> JSON_FIELD_INDEX =
            ConfigOptions.key("json-field-index")
                    .intType()
                    .noDefaultValue()
                    .withDescription("json field index, 0-based");

}
