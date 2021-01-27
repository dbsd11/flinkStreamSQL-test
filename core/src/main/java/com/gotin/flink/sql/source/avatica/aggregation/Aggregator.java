package com.gotin.flink.sql.source.avatica.aggregation;

import org.apache.flink.api.java.tuple.Tuple;

import java.util.stream.Stream;

public interface Aggregator {

    public Stream<Tuple> aggregation(Stream<Tuple> tupleStream);
}
