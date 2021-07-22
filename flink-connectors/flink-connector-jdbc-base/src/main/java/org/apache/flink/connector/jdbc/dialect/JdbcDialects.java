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

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Default JDBC dialects.
 */
public final class JdbcDialects {

    private static final List<JdbcDialect> DIALECTS = Arrays.asList(
            new DerbyDialect(),
            new MySQLDialect(),
            new PostgresDialect()
    );

    private static final List<LazyDialect> LAZY_DIALECTS = Arrays.asList(
            new LazyDialect("jdbc:aerospike:", "org.apache.flink.connector.jdbc.dialect.AerospikeDialect"),
            new LazyDialect("jdbc:clickhouse:", "org.apache.flink.connector.jdbc.dialect.ClickHouseDialect")
    );

    /**
     * Fetch the JdbcDialect class corresponding to a given database url.
     */
    public static Optional<JdbcDialect> get(String url) {
        for (JdbcDialect dialect : DIALECTS) {
            if (dialect.canHandle(url)) {
                return Optional.of(dialect);
            }
        }
        for(LazyDialect ld : LAZY_DIALECTS) {
            if (ld.canHandle(url)) {
                return Optional.of(ld.getDialect());
            }
        }
        return Optional.empty();
    }

    static class LazyDialect {
        private JdbcDialect dialect;
        private String jdbcScheme;
        private String dialectClass;

        public LazyDialect(String jdbcScheme, String dialectClass) {
            this.jdbcScheme = jdbcScheme;
            this.dialectClass = dialectClass;
        }

        public boolean canHandle(String url) {
            return url.startsWith(jdbcScheme);
        }

        public JdbcDialect getDialect() {
            try {
                if (dialect == null) {
                    dialect = (JdbcDialect) Class.forName(dialectClass, true, Thread.currentThread().getContextClassLoader()).newInstance();
                }
                return dialect;
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
