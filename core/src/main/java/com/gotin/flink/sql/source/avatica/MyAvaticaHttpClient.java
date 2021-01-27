package com.gotin.flink.sql.source.avatica;

import com.gotin.flink.sql.source.avatica.aggregation.Aggregator;
import com.gotin.flink.sql.source.avatica.aggregation.GroupConcatAggregator;
import com.gotin.flink.sql.source.avatica.aggregation.GroupConcatJsonAggregator;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.remote.AvaticaCommonsHttpClientImpl;
import org.apache.calcite.avatica.remote.Service;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class MyAvaticaHttpClient extends AvaticaCommonsHttpClientImpl {

    //这个ObjectMapper比较特殊，必须得使用flink集成的
    private ObjectMapper avatiaObjectMapper = new ObjectMapper();

    public MyAvaticaHttpClient(URL url) {
        super(url);
    }

    @Override
    public byte[] send(byte[] request) {
        byte[] response = super.send(request);
        try {
            Service.Response resp = avatiaObjectMapper.readValue(response, Service.Response.class);
            if ((resp instanceof Service.ExecuteResponse) && ((Service.ExecuteResponse) resp).missingStatement) {
                Service.ExecuteRequest executeRequest = avatiaObjectMapper.readValue(request, Service.ExecuteRequest.class);
                Service.PrepareResponse prepareResponse = prepare(new Service.PrepareRequest(executeRequest.statementHandle.connectionId, executeRequest.statementHandle.signature.sql, -1));

                Field statementIdField = Meta.StatementHandle.class.getDeclaredField("id");
                statementIdField.setAccessible(true);
                statementIdField.setInt(executeRequest.statementHandle, prepareResponse.statement.id);

                byte[] newStatementRequest = avatiaObjectMapper.writeValueAsBytes(executeRequest);
                response = super.send(newStatementRequest);
            } else if ((resp instanceof Service.ExecuteResponse) && CollectionUtils.isNotEmpty(((Service.ExecuteResponse) resp).results)) {
                String executeSql = ((Service.ExecuteResponse) resp).results.get(0).signature.sql;
                String sqlCommentInfo = executeSql.startsWith("--") ? executeSql.substring(0, executeSql.lastIndexOf("--")) : null;
                if (StringUtils.isNotEmpty(sqlCommentInfo) && sqlCommentInfo.toLowerCase().contains("aggregator")) {
                    List<String> columnList = ((Service.ExecuteResponse) resp).results.get(0).signature.columns.stream().map(columnMetaData -> columnMetaData.columnName).collect(Collectors.toList());
                    List<List> rowList = IteratorUtils.toList(((Service.ExecuteResponse) resp).results.get(0).firstFrame.rows.iterator());
                    List<Tuple> tupleList = new LinkedList<>();
                    Tuple headTuple = Tuple.newInstance(columnList.size());
                    for (int i = 0; i < columnList.size(); i++) {
                        headTuple.setField(columnList.get(i), i);
                    }
                    tupleList.add(headTuple);

                    tupleList.addAll(rowList.stream().map(row -> {
                        Tuple rowTuple = Tuple.newInstance(columnList.size());
                        for (int i = 0; i < row.size(); i++) {
                            rowTuple.setField(row.get(i), i);
                        }
                        return rowTuple;
                    }).collect(Collectors.toList()));

                    JsonNode aggregatorJson = avatiaObjectMapper.readTree(sqlCommentInfo.replaceAll("-", "").trim());
                    String aggregatorCls = aggregatorJson.get("class").asText();
                    Aggregator aggregator = aggregatorCls.contains("GroupConcatAggregator") ? new GroupConcatAggregator(aggregatorJson.get("concatField").asText()) :
                            aggregatorCls.contains("GroupConcatJsonAggregator") ? new GroupConcatJsonAggregator(aggregatorJson.get("concatField").asText(), aggregatorJson.get("jsonKey").asText()) : null;
                    List<List> newRowList = aggregator == null ? Collections.emptyList() : aggregator.aggregation(tupleList.stream()).skip(1).map(tuple -> {
                        List row = new ArrayList(tuple.getArity());
                        for (int i = 0; i < tuple.getArity(); i++) {
                            row.add(tuple.getField(i));
                        }
                        return row;
                    }).collect(Collectors.toList());

                    if (CollectionUtils.isNotEmpty(newRowList)) {
                        Field rowsField = Meta.Frame.class.getDeclaredField("rows");
                        rowsField.setAccessible(true);
                        rowsField.set(((Service.ExecuteResponse) resp).results.get(0).firstFrame, newRowList);

                        response = avatiaObjectMapper.writeValueAsBytes(resp);
                    }
                }
            }

        } catch (Exception e) {
            System.out.println("MyAvaticaHttpClient send optimize failed, return origin response");
        }

        return response;
    }

    Service.PrepareResponse prepare(Service.PrepareRequest prepareRequest) throws IOException {
        byte[] sendResult = this.send(avatiaObjectMapper.writeValueAsBytes(prepareRequest));
        Service.Response resp = avatiaObjectMapper.readValue(sendResult, Service.Response.class);
        if (resp instanceof Service.ErrorResponse) {
            throw ((Service.ErrorResponse) resp).toException();
        } else if (!Service.PrepareResponse.class.isAssignableFrom(resp.getClass())) {
            throw new ClassCastException("Cannot cast " + resp.getClass() + " into Service.PrepareResponse");
        } else {
            return Service.PrepareResponse.class.cast(resp);
        }
    }
}
