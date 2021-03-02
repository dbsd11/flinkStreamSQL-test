package com.gotin.flink.sql.sink.hologres.test;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;

import java.io.Serializable;
import java.util.Iterator;

public class Test01 {

    public static void main(String[] args) throws Exception {
        if (args != null && args[0].equals("testWrite")) {
            testWrite();
        } else if (args != null && args[0].equals("testRead")) {
        }
    }

    static void testWrite() throws Exception {
        StreamExecutionEnvironment writeEnv = StreamExecutionEnvironment.createLocalEnvironment();
        writeEnv.enableCheckpointing(1000, CheckpointingMode.AT_LEAST_ONCE);

        DataStream<RowData> input = writeEnv.fromCollection(new MyRowDataIterator(), RowData.class);

        String endPoint = "";
        String userName = "";
        String password = "";
        String database = "";
        String tableName = "";

        TableSchema schema = TableSchema.builder()
                .field("i", DataTypes.BIGINT()).build();

        StreamTableEnvironment tableEnv = StreamTableEnvironmentImpl.create(writeEnv, EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build(), new TableConfig());

        String CREATE_HOLOGRES_TABLE = String.format(
                "create table test_i(" +
                        "  i bigint" +
                        ") with (" +
                        "  'connector.type'='hologres'," +
                        "  'connector.database' = '%s'," +
                        "  'connector.table' = '%s'," +
                        "  'connector.username' = '%s'," +
                        "  'connector.password' = '%s'," +
                        "  'connector.endpoint' = '%s'," +
                        "  'connector.proxy_sink' = 'true'" +
                        ")", database, tableName, userName, password, endPoint);
        // Using a random data generator, and write to Hologres directly
        tableEnv.sqlUpdate(CREATE_HOLOGRES_TABLE);

        tableEnv.insertInto("test_i", tableEnv.fromDataStream(input));

        tableEnv.execute("Hologres Sink example");
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
