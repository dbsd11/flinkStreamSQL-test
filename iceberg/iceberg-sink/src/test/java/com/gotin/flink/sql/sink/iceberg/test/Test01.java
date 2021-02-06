package com.gotin.flink.sql.sink.iceberg.test;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkCatalogFactory;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.source.FlinkSource;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;

import java.io.Serializable;
import java.util.Iterator;

public class Test01 {

    public static void main(String[] args) throws Exception {
        HadoopCatalog catalog = new HadoopCatalog(FlinkCatalogFactory.clusterHadoopConf(), "oss://group-bison/test-iceberg/warehouse");
        boolean namespaceExists = catalog.listNamespaces().stream().anyMatch(namespace -> namespace.toString().equals("test"));
        if (!namespaceExists) {
            catalog.createNamespace(Namespace.of("test"));
        }
        boolean testTableExists = catalog.listTables(Namespace.of("test")).stream().anyMatch(tableIdentifier -> tableIdentifier.name().equals("test01"));
        if (!testTableExists) {
            catalog.createTable(TableIdentifier.of("test", "test01"), new Schema(Types.NestedField.required(1, "value", Types.StringType.get())));
        }
        catalog.loadTable(TableIdentifier.of("test", "test01"))
                .updateProperties()
                .set("write.format.default", "parquet")
                .set("write.target-file-size-bytes", "102400")
                .set("write.parquet.row-group-size-bytes", "102400")
                .set("write.parquet.page-size-bytes", "102400")
                .set("write.parquet.dict-size-bytes", "102400")
                .commit();
        catalog.close();

        if (args != null && args[0].equals("testWrite")) {
            testWrite();
        } else if (args != null && args[0].equals("testRead")) {
            testRead();
        }
    }

    static void testWrite() throws Exception {
        StreamExecutionEnvironment writeEnv = StreamExecutionEnvironment.createLocalEnvironment();
        writeEnv.enableCheckpointing(1000, CheckpointingMode.AT_LEAST_ONCE);

        DataStream<RowData> input = writeEnv.fromCollection(new MyRowDataIterator(), RowData.class);
        FlinkSink.forRowData(input)
                .tableLoader(TableLoader.fromHadoopTable("oss://group-bison/test-iceberg/warehouse/test/test01"))
                .overwrite(false)
                .build();
        writeEnv.execute("Test Write Iceberg DataStream");
    }

    static void testRead() throws Exception {
        StreamExecutionEnvironment readEnv = StreamExecutionEnvironment.createLocalEnvironment();
        FlinkSource.forRowData()
                .env(readEnv)
                .tableLoader(TableLoader.fromHadoopTable("oss://gotin-flink-test/iceberg/warehouse/test/test01"))
                .build()
                .print();
        readEnv.execute("Test read Iceberg DataStream");
    }

    static class MyRowDataIterator implements Iterator<RowData>, Serializable {
        private static final Long serialVersionUID = -1L;

        int i = 0;

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public RowData next() {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
            }

            GenericRowData rowData = new GenericRowData(RowKind.INSERT, 1);
            rowData.setField(0, StringData.fromString(String.valueOf(i++)));
            return rowData;
        }
    }
}
