package com.gotin.flink.sql.source.druidio.table;

import com.dtstack.flink.sql.table.AbstractSourceTableInfo;
import com.google.common.base.Preconditions;

/**
 * @Author: bison
 * @Date: 20/9/16
 */
public class DruidioTableInfo extends AbstractSourceTableInfo {
    private String connector;
    private String driver;
    private String url;
    private String username;
    private String password;
    private String rawQuery;

    public String getConnector() {
        return connector;
    }

    public void setConnector(String connector) {
        this.connector = connector;
    }

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

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

    public String getRawQuery() {
        return rawQuery;
    }

    public void setRawQuery(String rawQuery) {
        this.rawQuery = rawQuery;
    }

    @Override
    public boolean check() {
        Preconditions.checkNotNull(url, "Druidio field of url is required");
        Preconditions.checkNotNull(rawQuery, "Druidio field of rawQuery is required");
        return true;
    }

    @Override
    public String getType() {
        return "druidio";
    }
}
