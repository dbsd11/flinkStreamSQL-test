package com.gotin.flink.sql.sink.iceberg;

import com.dtstack.flink.sql.sink.IStreamSinkGener;
import com.dtstack.flink.sql.table.AbstractTargetTableInfo;
import com.dtstack.flink.sql.util.RowDataConvert;
import com.gotin.flink.sql.sink.iceberg.table.IcebergTableInfo;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkCatalogFactory;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.hadoop.HadoopCatalog;

import java.util.concurrent.atomic.AtomicReference;

/**
 * @Author: bison
 * @Date: 20/9/16
 */
public class IcebergSink implements RetractStreamTableSink<Row>, IStreamSinkGener<IcebergSink> {

    private IcebergTableInfo icebergTableInfo;
    protected String[] fieldNames;
    TypeInformation<?>[] fieldTypes;

    @Override
    public IcebergSink genStreamSink(AbstractTargetTableInfo targetTableInfo) {
        this.icebergTableInfo = (IcebergTableInfo) targetTableInfo;
        return this;
    }

    @Override
    public TypeInformation<Row> getRecordType() {
        return new RowTypeInfo(fieldTypes, fieldNames);
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
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

            boolean namespaceExists = catalog.listNamespaces().stream().anyMatch(namespace -> namespace.toString().equals(icebergeNamespaceRef.get()));
            if (!namespaceExists) {
                catalog.createNamespace(Namespace.of(icebergeNamespaceRef.get()));
            }
            boolean tableExists = catalog.listTables(Namespace.of(icebergeNamespaceRef.get())).stream().anyMatch(tableIdentifier -> tableIdentifier.name().equals(icebergeTableRef.get()));
            if (!tableExists) {
                catalog.createTable(TableIdentifier.of(icebergeNamespaceRef.get(), icebergeTableRef.get()), FlinkSchemaUtil.convert(getTableSchema()));
            }
            catalog.loadTable(TableIdentifier.of(icebergeNamespaceRef.get(), icebergeTableRef.get()))
                    .updateProperties()
                    .set("write.format.default", icebergTableInfo.getWriteFormat())
                    .set("write.target-file-size-bytes", String.valueOf(icebergTableInfo.getWriteDataFileBatch()))
                    .set("write.parquet.row-group-size-bytes", String.valueOf(icebergTableInfo.getWriteDataFileBatch()))
                    .set("write.parquet.page-size-bytes", String.valueOf(icebergTableInfo.getWriteDataFileBatch()))
                    .set("write.parquet.dict-size-bytes", String.valueOf(icebergTableInfo.getWriteDataFileBatch()))
                    .commit();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        DataStream<RowData> rowDataStream = dataStream.map(new MyRowDataMapFunction());
        DataStreamSink<RowData> dataStreamSink = FlinkSink.forRowData(rowDataStream)
                .tableLoader(TableLoader.fromHadoopTable(icebergTableInfo.getWarehouseLocation()))
                .writeParallelism(icebergTableInfo.getParallelism())
                .overwrite(false)
                .build();
        RichSinkFunction richSinkFunction = new OutputFormatSinkFunction(new IcebergOutputFormat(icebergTableInfo, fieldNames, fieldTypes));
        dataStream.addSink(richSinkFunction);
        return dataStreamSink;
    }

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        return this;
    }

    @Override
    public TupleTypeInfo<Tuple2<Boolean, Row>> getOutputType() {
        return new TupleTypeInfo(org.apache.flink.table.api.Types.BOOLEAN(), getRecordType());
    }

    @Override
    public String[] getFieldNames() {
        return fieldNames;
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return fieldTypes;
    }

    public static class MyRowDataMapFunction implements MapFunction<Tuple2<Boolean, Row>, RowData> {

        @Override
        public RowData map(Tuple2<Boolean, Row> row) throws Exception {
            return BooleanUtils.isNotFalse(row.f0) ? RowDataConvert.convertToBaseRow(row.f1) : null;
        }
    }
}
