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

package com.gotin.flink.sql.source.iceberg;

import com.dtstack.flink.sql.source.IStreamSourceGener;
import com.dtstack.flink.sql.table.AbstractSourceTableInfo;
import com.gotin.flink.sql.source.iceberg.table.IcebergTableInfo;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkCatalogFactory;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;
import org.apache.iceberg.hadoop.HadoopCatalog;

import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

/**
 * @author: bison
 * @create: 2020/01/07
 * @description:
 **/
public class IcebergSource implements IStreamSourceGener<Table> {

    @Override
    public Table genStreamSource(AbstractSourceTableInfo sourceTableInfo, StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        IcebergTableInfo icebergTableInfo = (IcebergTableInfo) sourceTableInfo;
        String tableLocation = null;

        //确保table存在，没有则新建
        try (HadoopCatalog catalog = new HadoopCatalog(FlinkCatalogFactory.clusterHadoopConf(), icebergTableInfo.getWarehouseLocation())) {
            final AtomicReference<String> icebergeNamespaceRef = new AtomicReference<>();
            final AtomicReference<String> icebergeTableRef = new AtomicReference<>(icebergTableInfo.getIcebergTableName());
            if (icebergeTableRef.get().contains("`.")) {
                icebergeNamespaceRef.set(icebergeTableRef.get().substring(1, icebergeTableRef.get().lastIndexOf("`")));
                icebergeTableRef.set(icebergeTableRef.get().substring(icebergeTableRef.get().lastIndexOf("`") + 1));
            } else {
                icebergeNamespaceRef.set("common");
            }
            tableLocation = String.join("", icebergTableInfo.getWarehouseLocation(), "/", icebergeNamespaceRef.get(), "/", icebergeTableRef.get());

            boolean namespaceExists = catalog.listNamespaces().stream().anyMatch(namespace -> namespace.toString().equals(icebergeNamespaceRef.get()));
            if (!namespaceExists) {
                catalog.createNamespace(Namespace.of(icebergeNamespaceRef.get()));
            }
            boolean tableExists = catalog.listTables(Namespace.of(icebergeNamespaceRef.get())).stream().anyMatch(tableIdentifier -> tableIdentifier.name().equals(icebergeTableRef.get()));
            if (!tableExists) {
                catalog.createTable(TableIdentifier.of(icebergeNamespaceRef.get(), icebergeTableRef.get()), FlinkSchemaUtil.convert(getSchema(icebergTableInfo)));
            }
            catalog.loadTable(TableIdentifier.of(icebergeNamespaceRef.get(), icebergeTableRef.get()))
                    .updateProperties()
                    .set("write.format.default", icebergTableInfo.getReadFormat())
                    .commit();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        env.setParallelism(sourceTableInfo.getParallelism());

        DataStream<RowData> rowDataStreamSource = FlinkSource.forRowData()
                .env(env)
                .project(getSchema(icebergTableInfo))
                .tableLoader(TableLoader.fromHadoopTable(tableLocation))
                .build();
        DataStream<Tuple2<Boolean, Row>> rowStreamSource = rowDataStreamSource.map(new MyRowDataMapFunction(new DataFormatConverters.RowConverter(getSchema(icebergTableInfo).getFieldDataTypes())));
        return tableEnv.fromDataStream(rowStreamSource);
    }

    protected TableSchema getSchema(IcebergTableInfo icebergTableInfo) {
        String[] fieldTypes = icebergTableInfo.getFieldTypes();
        Class<?>[] fieldClasses = icebergTableInfo.getFieldClasses();
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
                .fields(icebergTableInfo.getFields(), types)
                .build();
    }

    DataType getDataTypeFromClass(Class atomicCls) {
        DataType dataType = null;
        if (atomicCls.getSimpleName().toLowerCase().contains("string")) {
            dataType = DataTypes.STRING();
        } else if (atomicCls.getSimpleName().toLowerCase().contains("int")) {
            dataType = DataTypes.INT();
        } else if (atomicCls.getSimpleName().toLowerCase().contains("long")) {
            dataType = DataTypes.BIGINT();
        } else if (atomicCls.getSimpleName().toLowerCase().contains("float")) {
            dataType = DataTypes.FLOAT();
        } else if (atomicCls.getSimpleName().toLowerCase().contains("double")) {
            dataType = DataTypes.DOUBLE();
        } else if (atomicCls.getSimpleName().toLowerCase().contains("boolean")) {
            dataType = DataTypes.BOOLEAN();
        } else if (atomicCls.getSimpleName().toLowerCase().contains("byte[]")) {
            dataType = DataTypes.BYTES();
        } else if (atomicCls.getSimpleName().toLowerCase().contains("timestamp")) {
            dataType = DataTypes.TIMESTAMP();
        } else if (atomicCls.getSimpleName().toLowerCase().contains("date")) {
            dataType = DataTypes.DATE();
        }
        return dataType;
    }

    public static class MyRowDataMapFunction implements MapFunction<RowData, Tuple2<Boolean, Row>> {
        private DataFormatConverters.RowConverter rowConverter;

        public MyRowDataMapFunction() {
        }

        public MyRowDataMapFunction(DataFormatConverters.RowConverter rowConverter) {
            this.rowConverter = rowConverter;
        }

        @Override
        public Tuple2<Boolean, Row> map(RowData rowData) throws Exception {
            return rowConverter != null ? Tuple2.of(true, rowConverter.toExternal(rowData)) : Tuple2.of(false, null);
        }
    }
}
