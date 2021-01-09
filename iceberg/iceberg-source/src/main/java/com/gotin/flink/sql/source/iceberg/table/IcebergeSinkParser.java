package com.gotin.flink.sql.source.iceberg.table;

import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.table.AbstractTableParser;
import com.dtstack.flink.sql.util.MathUtil;

import java.util.Map;

import static com.dtstack.flink.sql.table.AbstractTableInfo.PARALLELISM_KEY;

/**
 * @Author: bison
 * @Date: 21/01/08
 */
public class IcebergeSinkParser extends AbstractTableParser {

    public static final String WAREHOUSE_LOCATION_KEY = "warehouseLocation";
    public static final String ICEBERG_TABLE_NAME_KEY = "icebergTableName";
    private static final String READ_FORMAT_KEY = "readFormat";

    @Override
    public AbstractTableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) throws Exception {
        IcebergTableInfo icebergTableInfo = new IcebergTableInfo();
        icebergTableInfo.setName(tableName);

        parseFieldsInfo(fieldsInfo, icebergTableInfo);

        icebergTableInfo.setParallelism(MathUtil.getIntegerVal(props.get(PARALLELISM_KEY.toLowerCase())));
        icebergTableInfo.setWarehouseLocation(MathUtil.getString(props.get(WAREHOUSE_LOCATION_KEY)));
        icebergTableInfo.setIcebergTableName(MathUtil.getString(props.get(ICEBERG_TABLE_NAME_KEY)));
        icebergTableInfo.setReadFormat(MathUtil.getString(props.getOrDefault(READ_FORMAT_KEY, "parquet")));

        return icebergTableInfo;
    }
}
