package com.gotin.flink.sql.sink.iceberg;

import com.dtstack.flink.sql.outputformat.AbstractDtRichOutputFormat;
import com.gotin.flink.sql.sink.iceberg.table.IcebergTableInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @Author: bison
 * @Date: 20/9/16
 */
public class IcebergOutputFormat extends AbstractDtRichOutputFormat<Tuple2> {
    private static final long serialVersionUID = -7994311331389155692L;

    private static final Logger LOG = LoggerFactory.getLogger(IcebergOutputFormat.class);

    private IcebergTableInfo icebergTableInfo;
    protected String[] fieldNames;
    TypeInformation<?>[] fieldTypes;

    public IcebergOutputFormat(IcebergTableInfo icebergTableInfo, String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        this.icebergTableInfo = icebergTableInfo;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
    }

    @Override
    public void configure(Configuration parameters) {
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        initMetric();
    }

    @Override
    public void writeRecord(Tuple2 record) throws IOException {
        Tuple2 tupleTrans = record;
        Boolean retract = (Boolean) tupleTrans.getField(0);
        Object row = tupleTrans.getField(1);
        try {
            if (retract) {
                if (outRecords.getCount() % ROW_PRINT_FREQUENCY == 0) {
                    LOG.info("Receive data : {}", row);
                }

                outRecords.inc();
            } else {
                //do nothing
            }
        } catch (Exception e) {
            outDirtyRecords.inc();

            throw new IllegalArgumentException("iceberge writeRecord() failed", e);
        }
    }

    @Override
    public void close() throws IOException {
    }
}
