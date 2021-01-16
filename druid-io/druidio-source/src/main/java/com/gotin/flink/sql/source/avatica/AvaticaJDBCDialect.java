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

package com.gotin.flink.sql.source.avatica;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

public class AvaticaJDBCDialect implements JdbcDialect {
    public static final String AVATICA_JDBC_DRIVER = "org.apache.calcite.avatica.remote.Driver";

    private String preDefinedQuery;

    public AvaticaJDBCDialect() {
    }

    public AvaticaJDBCDialect(String preDefinedQuery) {
        this.preDefinedQuery = preDefinedQuery;
    }

    @Override
    public String dialectName() {
        return "avatica";
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:avatica:");
    }

    @Override
    public JdbcRowConverter getRowConverter(RowType rowType) {
        return null;
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of(AVATICA_JDBC_DRIVER);
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }

    @Override
    public String getSelectFromStatement(String tableName, String[] selectFields, String[] conditionFields) {
        if (StringUtils.isNotEmpty(preDefinedQuery)) {
            return preDefinedQuery;
        }

        String selectExpressions = (String) Arrays.stream(selectFields).map(this::quoteIdentifier).collect(Collectors.joining(", "));
        String fieldExpressions = (String) Arrays.stream(conditionFields).map((f) -> {
            return this.quoteIdentifier(f) + "=?";
        }).collect(Collectors.joining(" AND "));
        return "SELECT " + selectExpressions + " FROM " + this.quoteIdentifier(tableName) + (conditionFields.length > 0 ? " WHERE " + fieldExpressions : "");
    }
}