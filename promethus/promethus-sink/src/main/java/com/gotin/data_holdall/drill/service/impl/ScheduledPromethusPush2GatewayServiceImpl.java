package com.gotin.data_holdall.drill.service.impl;

import com.codahale.metrics.MetricRegistry;
import com.gotin.data_holdall.drill.common.MyGauge;
import com.gotin.data_holdall.drill.service.PromethusPush2GatewayService;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.PushGateway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @Author: bison
 * @Date: 20/9/16
 * <p>
 * 推送promethus数据实现
 */
public class ScheduledPromethusPush2GatewayServiceImpl implements PromethusPush2GatewayService {
    private static Logger log = LoggerFactory.getLogger(PromethusPush2GatewayService.class);

    private Long intervalInMillis;

    private PushGateway pushGateway;

    private CollectorRegistry promethusRegistry;

    private ScheduledExecutorService executorService;

    private Map<String, MyGauge> myGaugeMap = new ConcurrentHashMap<>(64);

    public ScheduledPromethusPush2GatewayServiceImpl(String pushHost, Long intervalInMillis) {
        this.intervalInMillis = intervalInMillis != null ? intervalInMillis : 100;

        this.pushGateway = new PushGateway(pushHost);

        MetricRegistry dropwizardRegistry = new MetricRegistry();

        CollectorRegistry prometheusCollectorRegistry = new CollectorRegistry();
        prometheusCollectorRegistry.register(new DropwizardExports(dropwizardRegistry));
        this.promethusRegistry = prometheusCollectorRegistry;

        afterPropertiesSet();
    }

    @Override
    public void addMetric(String jobName, String metricName, String[] labelNames, String[] labelValues, double value, long timestamp) {
        MyGauge myGauge = myGaugeMap.containsKey(jobName) ? myGaugeMap.get(jobName) : null;
        if (myGauge != null && !Arrays.equals(myGauge.getLabelNames(), labelNames)) {
            myGauge = null;
        }

        if (myGauge == null) {
            myGauge = MyGauge.builder().name(jobName).help(jobName)
                    .labelNames(labelNames)
                    .create();
            promethusRegistry.register(myGauge);

            myGaugeMap.put(jobName, myGauge);
        }

        myGauge.labels(labelValues)
                .setMetricName(metricName)
                .setTimestamp(Math.abs(timestamp - System.currentTimeMillis()) > 30 * 60 * 1000 ? System.currentTimeMillis() : timestamp)
                .set(value);
    }

    @Override
    public void pushAll() throws IOException {
        for (Map.Entry<String, MyGauge> myGaugeEntry : myGaugeMap.entrySet()) {
            pushGateway.pushAdd(myGaugeEntry.getValue(), myGaugeEntry.getKey());
        }
    }

    public void cancelSchedule() {
        try {
            executorService.awaitTermination(2000, TimeUnit.SECONDS);
        } catch (Exception e) {
            try {
                executorService.shutdownNow();
            } catch (Exception e1) {
            }
        }
    }

    public void afterPropertiesSet() {
        this.executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleAtFixedRate(() -> {
            try {
                pushAll();
            } catch (Exception e) {
                log.error("scheduled pushAll失败", e);
            }
        }, 100, intervalInMillis, TimeUnit.MILLISECONDS);
    }
}