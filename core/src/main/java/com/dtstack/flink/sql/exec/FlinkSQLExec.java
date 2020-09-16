/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flink.sql.exec;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.api.internal.SelectTableSink;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.delegation.StreamPlanner;
import org.apache.flink.table.planner.operations.SqlToOperationConverter;
import org.apache.flink.table.sinks.TableSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.UUID;


/**
 * @description:  mapping by name when insert into sink table
 * @author: maqi
 * @create: 2019/08/15 11:09
 */
public class FlinkSQLExec {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkSQLExec.class);

    public static void sqlUpdate(StreamTableEnvironment tableEnv, String stmt) throws Exception {
        StreamTableEnvironmentImpl tableEnvImpl = ((StreamTableEnvironmentImpl) tableEnv);
        StreamPlanner streamPlanner = (StreamPlanner)tableEnvImpl.getPlanner();
        FlinkPlannerImpl flinkPlanner = streamPlanner.createFlinkPlanner();

        RichSqlInsert insert = (RichSqlInsert) flinkPlanner.validate(flinkPlanner.parser().parse(stmt));
        TableImpl queryResult = extractQueryTableFromInsertCaluse(tableEnvImpl, flinkPlanner, insert);

        String targetTableName = ((SqlIdentifier) ((SqlInsert) insert).getTargetTable()).names.get(0);
        //todo fix

        SelectTableSink selectTableSink = streamPlanner.createSelectTableSink(queryResult.getSchema());
//        TableSink tableSink = getTableSinkByPlanner(streamPlanner, targetTableName);

        String[] sinkFieldNames = selectTableSink.getTableSchema().getFieldNames();
        String[] queryFieldNames = queryResult.getSchema().getFieldNames();

        if (sinkFieldNames.length != queryFieldNames.length) {
            throw new ValidationException(
                    "Field name of query result and registered TableSink " + targetTableName + " do not match.\n" +
                            "Query result schema: " + String.join(",", queryFieldNames) + "\n" +
                            "TableSink schema: " + String.join(",", sinkFieldNames));
        }


        Table newTable = null;
        try {
            newTable = queryResult.select(String.join(",", sinkFieldNames));
        } catch (Exception e) {
            throw new ValidationException(
                    "Field name of query result and registered TableSink "+targetTableName +" do not match.\n" +
                    "Query result schema: " + String.join(",", queryFieldNames) + "\n" +
                    "TableSink schema: " + String.join(",", sinkFieldNames));
        }

        try {
            tableEnv.insertInto(targetTableName, newTable);
        } catch (Exception e) {
            LOG.warn("Field name case of query result and registered TableSink do not match. ", e);
            newTable = queryResult.select(String.join(",", ignoreCase(queryFieldNames, sinkFieldNames)));
            tableEnv.insertInto(targetTableName, newTable);
        }

        tableEnv.execute(String.join("", "insert_", targetTableName, "_", UUID.randomUUID().toString()));
    }

    private static TableSink getTableSinkByPlanner(StreamPlanner streamPlanner, String targetTableName)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Method getTableSink = PlannerBase.class.getDeclaredMethod("getTableSink", ObjectIdentifier.class);
        getTableSink.setAccessible(true);
        ObjectIdentifier objectIdentifier = ObjectIdentifier.of(streamPlanner.catalogManager().getCurrentCatalog(), streamPlanner.catalogManager().getCurrentDatabase(), targetTableName);
        Option tableSinkOption = (Option) getTableSink.invoke(streamPlanner, objectIdentifier);
        return (TableSink) ((Tuple2) tableSinkOption.get())._2;
    }

    private static TableImpl extractQueryTableFromInsertCaluse(StreamTableEnvironmentImpl tableEnvImpl, FlinkPlannerImpl flinkPlanner, RichSqlInsert insert)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        StreamPlanner streamPlanner = (StreamPlanner) tableEnvImpl.getPlanner();
        Operation queryOperation = SqlToOperationConverter.convert(flinkPlanner, streamPlanner.catalogManager(), insert.getSource()).get();
        Method createTableMethod = TableEnvironmentImpl.class.getDeclaredMethod("createTable", QueryOperation.class);
        createTableMethod.setAccessible(true);
        return (TableImpl) createTableMethod.invoke(tableEnvImpl, queryOperation);
    }

    private static String[] ignoreCase(String[] queryFieldNames, String[] sinkFieldNames) {
        String[] newFieldNames = sinkFieldNames;
        for (int i = 0; i < newFieldNames.length; i++) {
            for (String queryFieldName : queryFieldNames) {
                if (newFieldNames[i].equalsIgnoreCase(queryFieldName)) {
                    newFieldNames[i] = queryFieldName;
                    break;
                }
            }
        }
        return newFieldNames;
    }
}