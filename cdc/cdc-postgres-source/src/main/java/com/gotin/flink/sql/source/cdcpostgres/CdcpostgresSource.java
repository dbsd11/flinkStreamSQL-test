package com.gotin.flink.sql.source.cdcpostgres;

import com.alibaba.ververica.cdc.connectors.postgres.table.PostgreSQLTableSource;
import com.alibaba.ververica.cdc.debezium.table.DebeziumOptions;
import com.dtstack.flink.sql.source.IStreamSourceGener;
import com.dtstack.flink.sql.table.AbstractSourceTableInfo;
import com.dtstack.flink.sql.util.RowDataConvert;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gotin.flink.sql.source.cdcpostgres.table.CdcpostgresTableInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Properties;
import java.util.stream.IntStream;

public class CdcpostgresSource implements IStreamSourceGener<Table> {

    public static DynamicTableSource cdcPostgresTableSource = null;

    private transient ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Table genStreamSource(AbstractSourceTableInfo sourceTableInfo, StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        CdcpostgresTableInfo cdcPostgresTableInfo = (CdcpostgresTableInfo) sourceTableInfo;

        env.setParallelism(sourceTableInfo.getParallelism());

        URI uri = URI.create(cdcPostgresTableInfo.getUrl().startsWith("jdbc:") ? cdcPostgresTableInfo.getUrl().substring("jdbc:".length()) : cdcPostgresTableInfo.getUrl());
        String hostname = uri.getHost();
        int port = uri.getPort();
        String databaseName = uri.getPath().contains("?") ? uri.getPath().substring(1, uri.getPath().indexOf("?")) : uri.getPath().substring(1);
        String username = cdcPostgresTableInfo.getUsername();
        String password = cdcPostgresTableInfo.getPassword();
        String schemaName = cdcPostgresTableInfo.getSchemaName();
        String tableName = cdcPostgresTableInfo.getTableName();
        String decodingPluginName = "decoderbufs";
        Properties debeziumProperties = null;
        try {
            debeziumProperties = DebeziumOptions.getDebeziumProperties(objectMapper.readValue(StringUtils.defaultString(cdcPostgresTableInfo.getDebeziumConf(), "{}"), HashMap.class));
        } catch (IOException e) {
        }

        TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(getSchema(sourceTableInfo));
        PostgreSQLTableSource cdcPostgresTableSource = new PostgreSQLTableSource(
                physicalSchema,
                port,
                hostname,
                databaseName,
                schemaName,
                tableName,
                username,
                password,
                decodingPluginName,
                debeziumProperties);
        CdcpostgresSource.cdcPostgresTableSource = cdcPostgresTableSource;

        SourceFunctionProvider cdcSourceProvider = (SourceFunctionProvider) cdcPostgresTableSource.getScanRuntimeProvider(new ScanRuntimeProviderContext());
        DataStream<RowData> cdcRowDataStream = env.addSource(cdcSourceProvider.createSourceFunction());
        DataStream<Row> cdcStream = cdcRowDataStream.map(rowData -> rowData instanceof GenericRowData ? RowDataConvert.convertToRow((GenericRowData) rowData) : null, physicalSchema.toRowType());
        return tableEnv.fromDataStream(cdcStream, String.join(",", sourceTableInfo.getFields()));
    }

    protected TableSchema getSchema(AbstractSourceTableInfo sourceTableInfo) {
        String[] fieldTypes = sourceTableInfo.getFieldTypes();
        Class<?>[] fieldClasses = sourceTableInfo.getFieldClasses();
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
                .fields(sourceTableInfo.getFields(), types)
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
}
