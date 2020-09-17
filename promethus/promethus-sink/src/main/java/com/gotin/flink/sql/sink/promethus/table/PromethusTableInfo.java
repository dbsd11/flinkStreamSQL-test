package com.gotin.flink.sql.sink.promethus.table;

import com.dtstack.flink.sql.table.AbstractTargetTableInfo;
import com.google.common.base.Preconditions;

/**
 * @Author: bison
 * @Date: 20/9/16
 */
public class PromethusTableInfo extends AbstractTargetTableInfo {

    private String pushHost;

    private Long pushIntervalInMillis;

    private Long pushRecordsBatch;

    public String getPushHost() {
        return pushHost;
    }

    public void setPushHost(String pushHost) {
        this.pushHost = pushHost;
    }

    public Long getPushIntervalInMillis() {
        return pushIntervalInMillis;
    }

    public void setPushIntervalInMillis(Long pushIntervalInMillis) {
        this.pushIntervalInMillis = pushIntervalInMillis;
    }

    public Long getPushRecordsBatch() {
        return pushRecordsBatch;
    }

    public void setPushRecordsBatch(Long pushRecordsBatch) {
        this.pushRecordsBatch = pushRecordsBatch;
    }

    @Override
    public boolean check() {
        Preconditions.checkNotNull(pushHost, "Promethus field of pushHost is required");
        return true;
    }

    @Override
    public String getType() {
        return "promethus";
    }
}
