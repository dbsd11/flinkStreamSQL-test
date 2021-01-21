package com.gotin.flink.sql.source.druidio.table;

import com.dtstack.flink.sql.table.AbstractSourceParser;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.util.MathUtil;
import com.gotin.flink.sql.source.avatica.AvaticaJDBCDialect;

import java.util.Map;

import static com.dtstack.flink.sql.table.AbstractTableInfo.PARALLELISM_KEY;

/**
 * @Author: bison
 * @Date: 21/01/08
 */
public class DruidioSourceParser extends AbstractSourceParser {

    private static final String CONNECTOR_KEY = "connector";
    private static final String DRIVER_KEY = "driver";
    private static final String URL_KEY = "url";
    private static final String USERNAME_KEY = "username";
    private static final String PASSWORD_KEY = "password";
    private static final String RAW_QUERY_KEY = "raw_query";

    @Override
    public AbstractTableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) throws Exception {
        DruidioTableInfo druidioTableInfo = new DruidioTableInfo();
        druidioTableInfo.setName(tableName);

        parseFieldsInfo(fieldsInfo, druidioTableInfo);

        druidioTableInfo.setParallelism(MathUtil.getIntegerVal(props.get(PARALLELISM_KEY.toLowerCase())));
        druidioTableInfo.setConnector(MathUtil.getString(props.getOrDefault(CONNECTOR_KEY, "jdbc")));
        druidioTableInfo.setDriver(MathUtil.getString(props.getOrDefault(DRIVER_KEY, AvaticaJDBCDialect.AVATICA_JDBC_DRIVER)));
        druidioTableInfo.setUrl(MathUtil.getString(props.get(URL_KEY)));
        druidioTableInfo.setUsername(MathUtil.getString(props.get(USERNAME_KEY)));
        druidioTableInfo.setPassword(MathUtil.getString(props.get(PASSWORD_KEY)));
        druidioTableInfo.setRawQuery(MathUtil.getString(props.get(RAW_QUERY_KEY)));
        return druidioTableInfo;
    }
}
