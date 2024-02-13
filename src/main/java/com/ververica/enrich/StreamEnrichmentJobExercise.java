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

package com.ververica.enrich;

import com.ververica.enrich.function.*;
import com.ververica.enrich.record.*;
import com.ververica.enrich.util.DatabaseClientParameter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Stream Enrichment Flink Job
 */
public class StreamEnrichmentJobExercise {
	private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(StreamEnrichmentJobExercise.class);
	private static ParameterTool params;
	private static DatabaseClientParameter dbClientParameter;
	private static StreamExecutionEnvironment env;

	public static void main(String[] args) throws Exception {

		/**
		 * Get command line parameters with ParameterTool
		 * See {@link #createDBClientParameter()} and {@link #createCustomerChangeLogStream()} for database connection
		 *  related parameters, and {@link #createOrderStream()} for kafka related parameters
		 *  that this program accepts and requires
		 */
		params = ParameterTool.fromArgs(args);

		dbClientParameter = createDBClientParameter();

		env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<Order> orderStream = createOrderStream();
		KeyedStream<Order, Integer> keyedOrderStream = orderStream.keyBy(o -> o.getCustomerId());

		DataStream<CustomerChangeLog> customerChangeLogStream = createCustomerChangeLogStream();
		KeyedStream<CustomerChangeLog, Integer> keyedCustomerChangeLogStream =
				customerChangeLogStream.keyBy( c -> c.getCustomer().getId() );

		// define pipeline based on the given enrich method
		DataStream<EnrichedOrder> enrichedStream;

		//////////////////////////////////////// Begin of your solution block //////////////////////////////////////////

		// Replace this block with your solution of a specific enrichment method
		// The following variables are available to you:
		//    orderStream, keyedOrderStream
		//    customerChangeLogStream, keyedCustomerChangeLogStream
		//    dbClientParameter

		// Here is an example of "per record look up (sync)"
		String enrichMethod = "PerRecordLookupSync";
		enrichedStream = orderStream.map(new EnrichOrderSyncFunction(dbClientParameter));

		//////////////////////////////////////// End of your solution block ////////////////////////////////////////////

		enrichedStream.print("Enriched");

		// Execute program, beginning computation.
		env.execute("Stream Enrichment via " + enrichMethod);
	}

	private static DataStreamSource<Order> createOrderStream() {

		String kafkaServer = params.get("bootstrap-servers","kafka:9092");
		String kafkaTopic = params.get("kafka-topic", "topic-name");
		String kafkaUser = params.get("kafka-user", "username");
		String kafkaPass = params.get("kafka-pass", "password");

		LOGGER.info(
				String.format("Using kafka: %s, topic: %s, specify --kafka and/or --topic if needed.",
						kafkaServer, kafkaTopic)
		);

		// order stream
		KafkaSource<Order> orderSource = KafkaSource.<Order>builder()
				.setBootstrapServers(kafkaServer)
				.setTopics(kafkaTopic)
				.setGroupId("my-group")
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new OrderDeserializer())
				.setProperty("security.protocol", "SASL_SSL")
				.setProperty("sasl.mechanism", "SCRAM-SHA-512")
				.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\""+kafkaUser+"\" password=\""+kafkaPass+"\";")
				.build();

		return env.fromSource(
				orderSource,
				WatermarkStrategy
						.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
						.withTimestampAssigner( (order, timestamp) -> order.getEventTimestamp() )
						.withIdleness(Duration.ofSeconds(3))
						.withWatermarkAlignment("enrichment",
								Duration.ofSeconds(30), Duration.ofSeconds(5)),
				"Kafka Source"
		);
	}

	private static DatabaseClientParameter createDBClientParameter() {
		return DatabaseClientParameter.builder()
				.host( params.get("mysql-host","mysql-host") )
				.port( params.getInt("mysql-port",3306) )
				.database( params.get("mysql-db","database") )
				.username( params.get("mysql-user","admin") )
				.password( params.get("mysql-pass","password") )
				.build();
	}

	private static DataStream<CustomerChangeLog> createCustomerChangeLogStream() {
		String binlogOffsetFile = params.get("binlog-offset-file", "mysql-bin-changelog.000005");

		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		String tableName = "customers";
		tableEnv.executeSql(String.format(
				"CREATE TABLE %s (" +
					"id INT," +
					"name STRING," +
					"country STRING," +
					"zipcode STRING," +
					"status STRING," +
					"updatedAt STRING," +
					"PRIMARY KEY (id) NOT ENFORCED" +
				") WITH (" +
					"'connector' = 'mysql-cdc'," +
					"'server-time-zone' = 'UTC'," +
					"'hostname' = '%s'," +
					"'port' = '%d'," +
					"'username' = '%s'," +
					"'password' = '%s'," +
					"'database-name' = '%s'," +
					"'table-name' = '.*'," +
					"'scan.startup.mode' = 'specific-offset'," +
					"'scan.startup.specific-offset.file' = '%s'," +
					"'scan.startup.specific-offset.pos' = '0'"	+
				")",
				tableName,
				dbClientParameter.getHost(),
				dbClientParameter.getPort(),
				dbClientParameter.getUsername(),
				dbClientParameter.getPassword(),
				dbClientParameter.getDatabase(),
				binlogOffsetFile
				)
		);

		Table customerTable = tableEnv.from(tableName);
		return tableEnv.toChangelogStream(customerTable)
				.map( r -> CustomerChangeLog.fromRow(r) )
				.assignTimestampsAndWatermarks(
						WatermarkStrategy
								.<CustomerChangeLog>forBoundedOutOfOrderness(Duration.ofSeconds(5))
								.withTimestampAssigner(
										( customerChangelog, timestamp) -> customerChangelog.getEventTimestamp() )
								.withIdleness(Duration.ofSeconds(3))
				);
	}

}
