package com.gotin.flink.sql.source.cdcpostgres.table;

import com.dtstack.flink.sql.table.AbstractSourceParser;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.util.MathUtil;

import java.util.Map;

import static com.dtstack.flink.sql.table.AbstractTableInfo.PARALLELISM_KEY;

/**
 * @Author: bison
 * @Date: 21/01/08
 */
public class CdcpostgresSourceParser extends AbstractSourceParser {
    private static final String URL_KEY = "url";
    private static final String USERNAME_KEY = "username";
    private static final String PASSWORD_KEY = "password";
    private static final String SCHEMA_NAME_KEY = "schemaname";
    private static final String TABLE_NAME_KEY = "tablename";
    private static final String DEBEZIUM_CONF_KEY = "debeziumconf";

    @Override
    public AbstractTableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) throws Exception {
        CdcpostgresTableInfo cdcPostgresTableInfo = new CdcpostgresTableInfo();
        cdcPostgresTableInfo.setName(tableName);

        parseFieldsInfo(fieldsInfo, cdcPostgresTableInfo);

        cdcPostgresTableInfo.setParallelism(MathUtil.getIntegerVal(props.get(PARALLELISM_KEY.toLowerCase())));
        cdcPostgresTableInfo.setUrl(MathUtil.getString(props.get(URL_KEY)));
        cdcPostgresTableInfo.setUsername(MathUtil.getString(props.get(USERNAME_KEY)));
        cdcPostgresTableInfo.setPassword(MathUtil.getString(props.get(PASSWORD_KEY)));
        cdcPostgresTableInfo.setSchemaName(MathUtil.getString(props.getOrDefault(SCHEMA_NAME_KEY, "public")));
        cdcPostgresTableInfo.setTableName(MathUtil.getString(props.get(TABLE_NAME_KEY)));
        cdcPostgresTableInfo.setDebeziumConf(MathUtil.getString(props.get(DEBEZIUM_CONF_KEY)));
        return cdcPostgresTableInfo;
    }
}
