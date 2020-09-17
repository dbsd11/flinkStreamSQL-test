package com.gotin.flink.sql.sink.promethus.table;

import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.table.AbstractTableParser;
import com.dtstack.flink.sql.util.MathUtil;

import java.util.Map;

import static com.dtstack.flink.sql.table.AbstractTableInfo.PARALLELISM_KEY;

/**
 * @Author: bison
 * @Date: 20/9/16
 */
public class PromethusSinkParser extends AbstractTableParser {

    public static final String PUSH_HOST_KEY = "pushHost";
    public static final String PUSH_INTERVAL_IN_MILLIS_KEY = "pushIntervalInMillis";
    public static final String PUSH_RECORDS_BATCH_KEY = "pushRecordsBatch";

    @Override
    public AbstractTableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) throws Exception {
        PromethusTableInfo promethusTableInfo = new PromethusTableInfo();
        promethusTableInfo.setName(tableName);

        parseFieldsInfo(fieldsInfo, promethusTableInfo);

        promethusTableInfo.setParallelism(MathUtil.getIntegerVal(props.get(PARALLELISM_KEY.toLowerCase())));
        promethusTableInfo.setPushHost(MathUtil.getString(props.get(PUSH_HOST_KEY.toLowerCase())));
        promethusTableInfo.setPushIntervalInMillis(MathUtil.getLongVal(props.get(PUSH_INTERVAL_IN_MILLIS_KEY.toLowerCase())));
        promethusTableInfo.setPushRecordsBatch(MathUtil.getLongVal(props.get(PUSH_RECORDS_BATCH_KEY.toLowerCase()), 100L));

        return promethusTableInfo;
    }
}
