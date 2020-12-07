/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flink.sql.sink.elasticsearch;

import com.dtstack.flink.sql.enums.EUpdateMode;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.types.Row;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Requests;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author yinxi
 * @date 2020/1/9 - 15:10
 */
public class CustomerSinkFunc implements ElasticsearchSinkFunction<Tuple2> {

    private final Logger logger = LoggerFactory.getLogger(CustomerSinkFunc.class);
    /**
     * 用作ID的属性值连接符号
     */
    private static final String ID_VALUE_SPLIT = "_";

    private String index;

    private String type;

    private List<Integer> idFieldIndexList;

    private List<String> fieldNames;

    private List<String> fieldTypes;

    private String updateMode;

    private transient ObjectMapper jsonObjectMapper = new ObjectMapper();

    private transient Counter outRecords;

    private transient Counter outDirtyRecords;

    public CustomerSinkFunc(String index, String type, List<String> fieldNames, List<String> fieldTypes, List<Integer> idFieldIndexes) {
        this.index = index;
        this.type = type;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.idFieldIndexList = idFieldIndexes;
    }

    public String getUpdateMode() {
        return updateMode;
    }

    public void setUpdateMode(String updateMode) {
        this.updateMode = updateMode;
    }

    @Override
    public void process(Tuple2 tuple2, RuntimeContext ctx, RequestIndexer indexer) {
        try {
            Tuple2<Boolean, Row> tupleTrans = tuple2;
            Boolean retract = tupleTrans.getField(0);
            Row element = tupleTrans.getField(1);
            if (!retract) {
                return;
            }

            if (StringUtils.equalsIgnoreCase(updateMode, EUpdateMode.UPSERT.name())) {
                indexer.add(createUpdateRequest(element));
            } else {
                indexer.add(createIndexRequest(element));
            }
            outRecords.inc();
        } catch (Throwable e) {
            outDirtyRecords.inc();
            logger.error("Failed to store source data {}. ", tuple2.getField(1));
            logger.error("Failed to create index request exception. ", e);
        }
    }

    public void setOutRecords(Counter outRecords) {
        this.outRecords = outRecords;
    }

    public void setOutDirtyRecords(Counter outDirtyRecords) {
        this.outDirtyRecords = outDirtyRecords;
    }

    private IndexRequest createIndexRequest(Row element) {
        String idFieldStr = "";
        if (null != idFieldIndexList) {
            // index start at 1,
            idFieldStr = idFieldIndexList.stream()
                    .filter(index -> index > 0 && index <= element.getArity())
                    .map(index -> element.getField(index - 1).toString())
                    .collect(Collectors.joining(ID_VALUE_SPLIT));
        }

        Map<String, Object> dataMap = Es6Util.rowToJsonMap(element, fieldNames, fieldTypes);
        int length = Math.min(element.getArity(), fieldNames.size());
        for (int i = 0; i < length; i++) {
            dataMap.put(fieldNames.get(i), element.getField(i));
            if (fieldNames.get(i).contains("json") && element.getField(i) instanceof String) {
                dataMap.put(fieldNames.get(i), fieldValueStr2Obj(fieldNames.get(i), (String) element.getField(i)));
            }
        }

        if (StringUtils.isEmpty(idFieldStr)) {
            return Requests.indexRequest()
                    .index(index)
                    .type(type)
                    .source(dataMap);
        }

        return Requests.indexRequest()
                .index(index)
                .type(type)
                .id(idFieldStr)
                .source(dataMap);
    }

    private UpdateRequest createUpdateRequest(Row element) {
        String idFieldStr = "";
        if (null != idFieldIndexList) {
            // index start at 1,
            idFieldStr = idFieldIndexList.stream()
                    .filter(index -> index > 0 && index <= element.getArity())
                    .map(index -> element.getField(index - 1).toString())
                    .collect(Collectors.joining(ID_VALUE_SPLIT));
        }

        Map<String, Object> dataMap = Es6Util.rowToJsonMap(element, fieldNames, fieldTypes);
        int length = Math.min(element.getArity(), fieldNames.size());
        for (int i = 0; i < length; i++) {
            dataMap.put(fieldNames.get(i), element.getField(i));
            if (fieldNames.get(i).contains("json") && element.getField(i) instanceof String) {
                dataMap.put(fieldNames.get(i), fieldValueStr2Obj(fieldNames.get(i), (String) element.getField(i)));
            }
        }

        if (StringUtils.isEmpty(idFieldStr)) {
            return new UpdateRequest()
                    .index(index)
                    .type(type)
                    .doc(dataMap)
                    .upsert(dataMap)
                    .docAsUpsert(true);
        }

        return new UpdateRequest()
                .index(index)
                .type(type)
                .id(idFieldStr)
                .doc(dataMap)
                .upsert(dataMap)
                .docAsUpsert(true);
    }

    Object fieldValueStr2Obj(String fieldName, String fieldValueStr) {
        if (StringUtils.isEmpty(fieldValueStr)) {
            return null;
        }

        if (jsonObjectMapper == null) {
            this.jsonObjectMapper = new ObjectMapper();
        }

        Object obj = null;
        if (fieldName.contains("jsonObj_")) {
            try {
                obj = jsonObjectMapper.readValue(fieldValueStr, HashMap.class);
            } catch (Exception e) {
                obj = new HashMap<>(0);
            }
        } else if (fieldName.contains("jsonArray_")) {
            try {
                obj = jsonObjectMapper.readValue(fieldValueStr, LinkedList.class);
            } catch (Exception e) {
                obj = new LinkedList<>();
            }
        }
        return obj;
    }
}
