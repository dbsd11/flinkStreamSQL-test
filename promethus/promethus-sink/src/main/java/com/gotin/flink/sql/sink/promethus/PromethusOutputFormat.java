package com.gotin.flink.sql.sink.promethus;

import com.dtstack.flink.sql.outputformat.AbstractDtRichOutputFormat;
import com.gotin.data_holdall.drill.service.PromethusPush2GatewayService;
import com.gotin.data_holdall.drill.service.impl.ScheduledPromethusPush2GatewayServiceImpl;
import com.gotin.flink.sql.sink.promethus.table.PromethusTableInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

/**
 * @Author: bison
 * @Date: 20/9/16
 */
public class PromethusOutputFormat extends AbstractDtRichOutputFormat<Tuple2> {
    private static final long serialVersionUID = -7994311331389155692L;

    private static final Logger LOG = LoggerFactory.getLogger(PromethusOutputFormat.class);

    private PromethusTableInfo promethusTableInfo;
    protected String[] fieldNames;
    TypeInformation<?>[] fieldTypes;

    private int metricNameIndex = 0;
    private int valueIndex = -1;
    private int timestampIndex = -1;

    private PromethusPush2GatewayService promethusPush2GatewayService;

    public PromethusOutputFormat(PromethusTableInfo promethusTableInfo, String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        this.promethusTableInfo = promethusTableInfo;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.valueIndex = fieldNames.length - 2;
        this.timestampIndex = fieldNames.length - 1;

        if (valueIndex <= metricNameIndex) {
            throw new IllegalArgumentException("fieldNames映射promethus metric失败");
        }
    }

    @Override
    public void configure(Configuration parameters) {
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        initMetric();

        if (this.promethusPush2GatewayService == null) {
            this.promethusPush2GatewayService = new ScheduledPromethusPush2GatewayServiceImpl(promethusTableInfo.getPushHost(), promethusTableInfo.getPushIntervalInMillis());
        }
    }

    @Override
    public void writeRecord(Tuple2 record) throws IOException {
        Tuple2<Boolean, Row> tupleTrans = record;
        Boolean retract = tupleTrans.getField(0);
        Row row = tupleTrans.getField(1);
        try {
            if (retract) {
                if (outRecords.getCount() % ROW_PRINT_FREQUENCY == 0) {
                    LOG.info("Receive data : {}", row);
                }

                doSend2Promethus(row);

                outRecords.inc();

                if (outRecords.getCount() % promethusTableInfo.getPushRecordsBatch() == 0) {
                    promethusPush2GatewayService.pushAll();

                    if (outRecords.getCount() % ROW_PRINT_FREQUENCY == 0) {
                        LOG.info("push all");
                    }
                }
            } else {
                //do nothing
            }
        } catch (Exception e) {
            outDirtyRecords.inc();

            throw new IllegalArgumentException("doSend2Promethus() failed", e);
        }
    }

    void doSend2Promethus(Row row) {
        String[] labelNames = Arrays.copyOfRange(fieldNames, metricNameIndex + 1, valueIndex);
        String[] labelValues = new String[valueIndex - metricNameIndex - 1];
        for (int i = 0; i < valueIndex - metricNameIndex - 1; i++) {
            labelValues[i] = String.valueOf(row.getField(metricNameIndex + 1 + i));
        }

        promethusPush2GatewayService.addMetric(promethusTableInfo.getName(), String.valueOf(row.getField(metricNameIndex)),
                labelNames, labelValues, row.getField(valueIndex) == null ? 0 : Double.valueOf(row.getField(valueIndex).toString()),
                row.getField(timestampIndex) == null ? System.currentTimeMillis() : Long.valueOf(row.getField(timestampIndex).toString()));
    }

    @Override
    public void close() throws IOException {
        if (promethusPush2GatewayService instanceof ScheduledPromethusPush2GatewayServiceImpl) {
            ((ScheduledPromethusPush2GatewayServiceImpl) promethusPush2GatewayService).cancelSchedule();
        }
    }
}
