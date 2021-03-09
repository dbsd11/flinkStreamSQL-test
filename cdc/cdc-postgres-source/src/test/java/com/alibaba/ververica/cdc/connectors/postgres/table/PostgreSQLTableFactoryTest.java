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

package com.alibaba.ververica.cdc.connectors.postgres.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.ExceptionUtils;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test for {@link PostgreSQLTableSource} created by {@link PostgreSQLTableFactory}.
 */
public class PostgreSQLTableFactoryTest {
	private static final TableSchema SCHEMA = TableSchema.builder()
		.field("aaa", DataTypes.INT().notNull())
		.field("bbb", DataTypes.STRING().notNull())
		.field("ccc", DataTypes.DOUBLE())
		.field("ddd", DataTypes.DECIMAL(31, 18))
		.field("eee", DataTypes.TIMESTAMP(3))
		.primaryKey("bbb", "aaa")
		.build();

	private static final String MY_LOCALHOST = "localhost";
	private static final String MY_USERNAME = "flinkuser";
	private static final String MY_PASSWORD = "flinkpw";
	private static final String MY_DATABASE = "myDB";
	private static final String MY_TABLE = "myTable";
	private static final String MY_SCHEMA = "public";
	private static final Properties PROPERTIES = new Properties();

	@Test
	public void testCommonProperties() {
		Map<String, String> properties = getAllOptions();

		// validation for source
		DynamicTableSource actualSource = createTableSource(properties);
		PostgreSQLTableSource expectedSource = new PostgreSQLTableSource(
			TableSchemaUtils.getPhysicalSchema(SCHEMA),
			5432,
			MY_LOCALHOST,
			MY_DATABASE,
			MY_SCHEMA,
			MY_TABLE,
			MY_USERNAME,
			MY_PASSWORD,
			"decoderbufs",
			PROPERTIES);
		assertEquals(expectedSource, actualSource);
	}

	@Test
	public void testOptionalProperties() {
		Map<String, String> options = getAllOptions();
		options.put("port", "5444");
		options.put("decoding.plugin.name", "wal2json");
		options.put("debezium.snapshot.mode", "never");

		DynamicTableSource actualSource = createTableSource(options);
		Properties dbzProperties = new Properties();
		dbzProperties.put("snapshot.mode", "never");
		PostgreSQLTableSource expectedSource = new PostgreSQLTableSource(
			TableSchemaUtils.getPhysicalSchema(SCHEMA),
			5444,
			MY_LOCALHOST,
			MY_DATABASE,
			MY_SCHEMA,
			MY_TABLE,
			MY_USERNAME,
			MY_PASSWORD,
			"wal2json",
			dbzProperties);
		assertEquals(expectedSource, actualSource);
	}

	@Test
	public void testValidation() {
		// validate illegal port
		try {
			Map<String, String> properties = getAllOptions();
			properties.put("port", "123b");

			createTableSource(properties);
			fail("exception expected");
		} catch (Throwable t) {
			assertTrue(ExceptionUtils.findThrowableWithMessage(t,
				"Could not parse value '123b' for key 'port'.").isPresent());
		}

		// validate missing required
		Factory factory = new PostgreSQLTableFactory();
		for (ConfigOption<?> requiredOption : factory.requiredOptions()) {
			Map<String, String> properties = getAllOptions();
			properties.remove(requiredOption.key());

			try {
				createTableSource(properties);
				fail("exception expected");
			} catch (Throwable t) {
				assertTrue(ExceptionUtils.findThrowableWithMessage(t,
					"Missing required options are:\n\n" + requiredOption.key()).isPresent());
			}
		}

		// validate unsupported option
		try {
			Map<String, String> properties = getAllOptions();
			properties.put("unknown", "abc");

			createTableSource(properties);
			fail("exception expected");
		} catch (Throwable t) {
			assertTrue(ExceptionUtils.findThrowableWithMessage(t,
				"Unsupported options:\n\nunknown").isPresent());
		}
	}

	private Map<String, String> getAllOptions() {
		Map<String, String> options = new HashMap<>();
		options.put("connector", "postgres-cdc");
		options.put("hostname", MY_LOCALHOST);
		options.put("database-name", MY_DATABASE);
		options.put("schema-name", MY_SCHEMA);
		options.put("table-name", MY_TABLE);
		options.put("username", MY_USERNAME);
		options.put("password", MY_PASSWORD);
		return options;
	}

	private static DynamicTableSource createTableSource(Map<String, String> options) {
		return FactoryUtil.createTableSource(
			null,
			ObjectIdentifier.of("default", "default", "t1"),
			new CatalogTableImpl(SCHEMA, options, "mock source"),
			new Configuration(),
			PostgreSQLTableFactoryTest.class.getClassLoader());
	}
}
