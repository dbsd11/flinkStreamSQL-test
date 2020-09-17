package com.gotin.flink.sql.sink.promethus;

import com.dtstack.flink.sql.sink.IStreamSinkGener;
import com.dtstack.flink.sql.table.AbstractTargetTableInfo;
import com.gotin.flink.sql.sink.promethus.table.PromethusTableInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

/**
 * @Author: bison
 * @Date: 20/9/16
 */
public class PromethusSink implements RetractStreamTableSink<Row>, IStreamSinkGener<PromethusSink> {

    private PromethusTableInfo promethusTableInfo;
    protected String[] fieldNames;
    TypeInformation<?>[] fieldTypes;

    @Override
    public PromethusSink genStreamSink(AbstractTargetTableInfo targetTableInfo) {
        this.promethusTableInfo = (PromethusTableInfo) targetTableInfo;
        return this;
    }

    @Override
    public TypeInformation<Row> getRecordType() {
        return new RowTypeInfo(fieldTypes, fieldNames);
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        RichSinkFunction richSinkFunction = new OutputFormatSinkFunction(new PromethusOutputFormat(promethusTableInfo, fieldNames, fieldTypes));
        DataStreamSink dataStreamSink = dataStream.addSink(richSinkFunction);
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
}
