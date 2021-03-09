package com.gotin.flink.sql.source.cdcpostgres.table;

import com.dtstack.flink.sql.table.AbstractSourceTableInfo;
import com.google.common.base.Preconditions;

/**
 * @Author: bison
 * @Date: 20/9/16
 */
public class CdcpostgresTableInfo extends AbstractSourceTableInfo {
    private String url;
    private String username;
    private String password;
    private String schemaName;
    private String tableName;
    private String debeziumConf;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getDebeziumConf() {
        return debeziumConf;
    }

    public void setDebeziumConf(String debeziumConf) {
        this.debeziumConf = debeziumConf;
    }

    @Override
    public boolean check() {
        Preconditions.checkNotNull(url, "CdcPostgres field of url is required");
        Preconditions.checkNotNull(tableName, "CdcPostgres field of tableName is required");
        return true;
    }

    @Override
    public String getType() {
        return "cdcpostgres";
    }
}
