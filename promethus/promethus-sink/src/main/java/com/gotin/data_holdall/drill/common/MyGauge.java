package com.gotin.data_holdall.drill.common;

import io.prometheus.client.Collector;
import io.prometheus.client.DoubleAdder;
import io.prometheus.client.GaugeMetricFamily;
import io.prometheus.client.SimpleCollector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @Author: bison
 * @Date: 20/9/17
 * <p>
 * 为了方便labelNameKey和timestamp的管理，自己实现gauge。
 */
public class MyGauge extends SimpleCollector<MyGauge.Child> implements Collector.Describable {

    private String jobName;
    private String[] labelNames;

    MyGauge(MyGauge.Builder b) {
        super(b);
        this.jobName = b.jobName;
        this.labelNames = b.labelNames;
    }

    public static MyGauge.Builder builder() {
        return new MyGauge.Builder();
    }

    public String getJobName() {
        return jobName;
    }

    public String[] getLabelNames() {
        return labelNames;
    }

    @Override
    protected MyGauge.Child newChild() {
        return new MyGauge.Child();
    }

    @Override
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples.Sample> samples = new ArrayList(this.children.size());
        Iterator var2 = this.children.entrySet().iterator();

        while (var2.hasNext()) {
            Map.Entry<List<String>, MyGauge.Child> c = (Map.Entry) var2.next();
            samples.add(new MetricFamilySamples.Sample(c.getValue().getMetricName(), super.labelNames, (List) c.getKey(), ((MyGauge.Child) c.getValue()).get(), c.getValue().getTimestamp()));
        }

        List<MetricFamilySamples> familySamplesList = this.familySamplesList(Type.GAUGE, samples);

        clear();

        return familySamplesList;
    }

    @Override
    public List<MetricFamilySamples> describe() {
        return Collections.singletonList(new GaugeMetricFamily(super.fullname, super.help, super.labelNames));
    }

    public static class Child {
        private String metricName;
        private Long timestamp;
        private final DoubleAdder value = new DoubleAdder();

        public Child setMetricName(String metricName) {
            this.metricName = metricName;
            return this;
        }

        public Child setTimestamp(Long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Child inc() {
            this.inc(1.0D);
            return this;
        }

        public Child inc(double amt) {
            this.value.add(amt);
            return this;
        }

        public Child dec() {
            this.dec(1.0D);
            return this;
        }

        public Child dec(double amt) {
            this.value.add(-amt);
            return this;
        }

        public Child set(double val) {
            this.value.set(val);
            return this;
        }

        public String getMetricName() {
            return metricName;
        }

        public Long getTimestamp() {
            return timestamp;
        }

        public double get() {
            return this.value.sum();
        }
    }

    public static class Builder extends io.prometheus.client.SimpleCollector.Builder<MyGauge.Builder, MyGauge> {
        private String jobName;
        private String[] labelNames;

        public Builder() {
        }

        @Override
        public MyGauge create() {
            return new MyGauge(this);
        }

        @Override
        public Builder name(String name) {
            this.jobName = name;
            return super.name(name);
        }

        @Override
        public Builder labelNames(String... labelNames) {
            this.labelNames = labelNames;
            return super.labelNames(labelNames);
        }
    }
}