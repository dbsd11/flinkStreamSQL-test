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

package com.gotin.flink.sql.source.druidio;

import com.dtstack.flink.sql.source.IStreamSourceGener;
import com.dtstack.flink.sql.table.AbstractSourceTableInfo;
import com.gotin.flink.sql.source.avatica.AvaticaJDBCDialect;
import com.gotin.flink.sql.source.druidio.table.DruidioTableInfo;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcReadOptions;
import org.apache.flink.connector.jdbc.table.JdbcTableSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.util.Base64;
import java.util.stream.IntStream;

/**
 * @author: bison
 * @create: 2020/01/15
 * @description:
 **/
public class DruidioSource implements IStreamSourceGener<Table> {

    @Override
    public Table genStreamSource(AbstractSourceTableInfo sourceTableInfo, StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        DruidioTableInfo druidioTableInfo = (DruidioTableInfo) sourceTableInfo;

        String query = new String(Base64.getDecoder().decode(druidioTableInfo.getRawQuery()));

        JdbcOptions jdbcOptions = JdbcOptions.builder()
                .setDriverName(druidioTableInfo.getDriver())
                .setDialect(new AvaticaJDBCDialect(query))
                .setDBUrl(druidioTableInfo.getUrl())
                .setUsername(druidioTableInfo.getUsername())
                .setPassword(druidioTableInfo.getPassword())
                .setTableName("empty")
                .build();

        JdbcReadOptions jdbcReadOptions = JdbcReadOptions.builder()
                .setFetchSize(100)
                .setQuery(query)
                .build();

        JdbcLookupOptions jdbcLookupOptions = JdbcLookupOptions.builder()
                .setCacheExpireMs(10000)
                .setCacheMaxSize(100000)
                .setMaxRetryTimes(3)
                .build();

        JdbcTableSource jdbcTableSource = JdbcTableSource.builder()
                .setOptions(jdbcOptions)
                .setReadOptions(jdbcReadOptions)
                .setLookupOptions(jdbcLookupOptions)
                .setSchema(getSchema(druidioTableInfo))
                .build();

        env.setParallelism(sourceTableInfo.getParallelism());

        tableEnv.registerTableSource(sourceTableInfo.getName(), jdbcTableSource);
        return null;
    }

    protected TableSchema getSchema(DruidioTableInfo druidioTableInfo) {
        String[] fieldTypes = druidioTableInfo.getFieldTypes();
        Class<?>[] fieldClasses = druidioTableInfo.getFieldClasses();
        DataType[] types =
                IntStream.range(0, fieldClasses.length)
                        .mapToObj(i -> {
                            if (fieldClasses[i].isArray()) {
                                return DataTypes.ARRAY(getDataTypeFromClass(fieldClasses[i].getComponentType()));
                            }
                            return getDataTypeFromClass(fieldClasses[i]);
                        })
                        .toArray(DataType[]::new);

        return TableSchema.builder()
                .fields(druidioTableInfo.getFields(), types)
                .build();
    }

    DataType getDataTypeFromClass(Class atomicCls) {
        DataType dataType = null;
        if (atomicCls.getSimpleName().toLowerCase().contains("string")) {
            dataType = DataTypes.STRING();
        } else if (atomicCls.getSimpleName().toLowerCase().contains("int")) {
            dataType = DataTypes.INT();
        } else if (atomicCls.getSimpleName().contains("long")) {
            dataType = DataTypes.BIGINT();
        } else if (atomicCls.getSimpleName().contains("float")) {
            dataType = DataTypes.FLOAT();
        } else if (atomicCls.getSimpleName().contains("double")) {
            dataType = DataTypes.DOUBLE();
        } else if (atomicCls.getSimpleName().contains("boolean")) {
            dataType = DataTypes.BOOLEAN();
        } else if (atomicCls.getSimpleName().contains("byte[]")) {
            dataType = DataTypes.BYTES();
        } else if (atomicCls.getSimpleName().contains("timestamp")) {
            dataType = DataTypes.TIMESTAMP();
        } else if (atomicCls.getSimpleName().contains("date")) {
            dataType = DataTypes.DATE();
        }
        return dataType;
    }

    public static class MyRowMapFunction implements MapFunction<Row, Tuple2<Boolean, Row>> {

        @Override
        public Tuple2<Boolean, Row> map(Row row) throws Exception {
            return Tuple2.of(true, row);
        }
    }
}
