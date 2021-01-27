package com.gotin.flink.sql.source.avatica.aggregation;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.java.tuple.Tuple;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class GroupConcatAggregator implements Aggregator {

    private String concatField;

    public GroupConcatAggregator(String concatField) {
        this.concatField = concatField;
    }

    @Override
    public Stream<Tuple> aggregation(Stream<Tuple> tupleStream) {
        List<Tuple> tupleList = tupleStream.collect(Collectors.toList());
        Tuple headTuple = CollectionUtils.isNotEmpty(tupleList) ? tupleList.get(0) : null;
        if (headTuple == null) {
            return Stream.empty();
        }

        final AtomicInteger concatFieldIndexAtom = new AtomicInteger(-1);
        for (int i = 0; i < headTuple.getArity(); i++) {
            String field = headTuple.getField(i);
            if (concatField.equalsIgnoreCase(field)) {
                concatFieldIndexAtom.set(i);
            }
        }

        Map<String, List<Tuple>> tuplesMap = tupleList.stream().skip(1).collect(Collectors.groupingBy((tuple) ->
                IntStream.range(0, tuple.getArity())
                        .filter(i -> i != concatFieldIndexAtom.get())
                        .mapToObj(i -> tuple.getField(i).toString())
                        .collect(Collectors.joining(","))));

        return Stream.concat(Stream.of(headTuple), tuplesMap.entrySet().stream().map(entry -> {
            if (CollectionUtils.isEmpty(entry.getValue())) {
                return null;
            }

            Tuple firstTuple = entry.getValue().get(0);
            Tuple concatTuple = Tuple.newInstance(firstTuple.getArity());
            for (int i = 0; i < concatTuple.getArity(); i++) {
                concatTuple.setField(firstTuple.getField(i), i);
            }
            String concatValue = entry.getValue().stream().map(tuple -> tuple.getField(concatFieldIndexAtom.get()).toString()).collect(Collectors.joining(","));
            concatTuple.setField(concatValue, concatFieldIndexAtom.get());
            return concatTuple;
        }).filter(tuple -> tuple != null));
    }
}
