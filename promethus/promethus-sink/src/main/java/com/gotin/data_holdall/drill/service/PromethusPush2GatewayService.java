package com.gotin.data_holdall.drill.service;

import java.io.IOException;

/**
 * @Author: bison
 * @Date: 20/9/16
 */
public interface PromethusPush2GatewayService {

    public void addMetric(String jobName, String metricName, String[] labelNames, String[] labelValues, double value, long timestamp);

    public void pushAll() throws IOException;
}
