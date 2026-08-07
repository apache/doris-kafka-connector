/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.doris.kafka.connector.e2e.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.doris.kafka.connector.cfg.DorisSinkConnectorConfig;
import org.apache.doris.kafka.connector.cfg.DorisTlsOptions;
import org.apache.doris.kafka.connector.e2e.doris.DorisCustomerServiceImpl;
import org.apache.doris.kafka.connector.e2e.kafka.KafkaContainerService;
import org.apache.doris.kafka.connector.e2e.kafka.KafkaContainerServiceImpl;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

/** Verifies Kafka Connect Stream Load against an externally supplied TLS Doris cluster. */
public class DorisTlsITCase {

    private static final String RUN_ID = UUID.randomUUID().toString().replace("-", "");
    private static final String DATABASE = "kafka_tls_it_" + RUN_ID;
    private static final String TABLE = "sink";
    private static final String TOPIC = "kafka_tls_it_" + RUN_ID;
    private static final String CONNECTOR_NAME = "kafka-tls-it-" + RUN_ID;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static DorisCustomerServiceImpl dorisService;
    private static KafkaContainerService kafkaService;
    private static KafkaProducer<String, String> producer;
    private static boolean databaseCreated;
    private static boolean connectorRegistered;

    @BeforeClass
    public static void setUp() throws Exception {
        Assume.assumeTrue(
                "External Doris is required",
                Boolean.parseBoolean(
                        System.getProperty(DorisCustomerServiceImpl.CUSTOMER_ENV, "false")));
        Assume.assumeTrue(
                "External Doris TLS is required",
                Boolean.parseBoolean(
                        System.getProperty(DorisCustomerServiceImpl.DORIS_ENABLE_TLS, "false")));
        dorisService = new DorisCustomerServiceImpl();
        Assume.assumeTrue(
                "External Doris HTTP TLS is required",
                dorisService.getTlsOptions().isEnabledFor(DorisTlsOptions.Protocol.HTTP));

        kafkaService = new KafkaContainerServiceImpl();
        kafkaService.startContainer();
        kafkaService.startConnector();

        dorisService.startContainer();

        Properties properties = new Properties();
        properties.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaService.getInstanceHostAndPort());
        properties.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(properties);

        executeSql("CREATE DATABASE " + DATABASE);
        databaseCreated = true;
        executeSql(
                "CREATE TABLE "
                        + DATABASE
                        + "."
                        + TABLE
                        + " (id INT, name STRING) ENGINE=OLAP UNIQUE KEY(id) "
                        + "DISTRIBUTED BY HASH(id) BUCKETS AUTO "
                        + "PROPERTIES ('replication_num' = '1')");
    }

    @AfterClass
    public static void tearDown() {
        if (kafkaService != null && connectorRegistered) {
            kafkaService.deleteKafkaConnector(CONNECTOR_NAME);
        }
        if (producer != null) {
            producer.close(Duration.ofSeconds(10));
        }
        if (dorisService != null && databaseCreated) {
            try {
                executeSql("DROP DATABASE IF EXISTS " + DATABASE);
            } finally {
                dorisService.close();
            }
        } else if (dorisService != null) {
            dorisService.close();
        }
    }

    @Test
    public void testKafkaConnectWritesToTlsDoris() throws Exception {
        producer.send(new ProducerRecord<>(TOPIC, "{\"id\":1,\"name\":\"tls-it\"}"))
                .get(30, TimeUnit.SECONDS);

        kafkaService.registerKafkaConnector(CONNECTOR_NAME, connectorConfig());
        connectorRegistered = true;
        waitForExpectedRow();
    }

    private static String connectorConfig() throws Exception {
        ObjectNode root = OBJECT_MAPPER.createObjectNode();
        root.put("name", CONNECTOR_NAME);
        ObjectNode config = root.putObject("config");
        config.put("connector.class", "org.apache.doris.kafka.connector.DorisSinkConnector");
        config.put("topics", TOPIC);
        config.put("tasks.max", "1");
        config.put("doris.topic2table.map", TOPIC + ":" + TABLE);
        config.put("buffer.count.records", "1");
        config.put("buffer.flush.time", "1");
        config.put("buffer.size.bytes", "1048576");
        config.put(DorisSinkConnectorConfig.DORIS_URLS, dorisService.getInstanceHost());
        config.put(
                DorisSinkConnectorConfig.DORIS_HTTP_PORT,
                String.valueOf(dorisService.getHttpPort()));
        config.put(
                DorisSinkConnectorConfig.DORIS_QUERY_PORT,
                String.valueOf(dorisService.getQueryPort()));
        config.put(DorisSinkConnectorConfig.DORIS_USER, dorisService.getUsername());
        config.put(DorisSinkConnectorConfig.DORIS_PASSWORD, dorisService.getPassword());
        config.put(DorisSinkConnectorConfig.DORIS_DATABASE, DATABASE);
        config.put("load.model", "stream_load");
        config.put("enable.2pc", "false");
        config.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        config.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        config.put("sink.properties.compress_type", "");

        DorisTlsOptions tlsOptions = dorisService.getTlsOptions();
        config.put(DorisSinkConnectorConfig.DORIS_ENABLE_TLS, tlsOptions.isEnabled());
        config.put(
                DorisSinkConnectorConfig.DORIS_TLS_CA_CERTIFICATE_PATH,
                tlsOptions.getCaCertificatePath());
        config.put(
                DorisSinkConnectorConfig.DORIS_TLS_SKIP_HOSTNAME_VERIFICATION,
                tlsOptions.isSkipHostnameVerification());
        config.put(
                DorisSinkConnectorConfig.DORIS_TLS_EXCLUDED_PROTOCOLS,
                System.getProperty(DorisCustomerServiceImpl.DORIS_TLS_EXCLUDED_PROTOCOLS, ""));
        return OBJECT_MAPPER.writeValueAsString(root);
    }

    private static void waitForExpectedRow() throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(2);
        String lastState = null;
        SQLException lastQueryError = null;
        while (System.nanoTime() < deadline) {
            lastState = kafkaService.getConnectorTaskStatus(CONNECTOR_NAME);
            if ("FAILED".equalsIgnoreCase(lastState)) {
                Assert.fail("Kafka Connect task failed");
            }
            try (Connection connection = dorisService.getQueryConnection();
                    Statement statement = connection.createStatement();
                    ResultSet resultSet =
                            statement.executeQuery(
                                    "SELECT id, name FROM " + DATABASE + "." + TABLE)) {
                if (resultSet.next()) {
                    Assert.assertEquals(1, resultSet.getInt(1));
                    Assert.assertEquals("tls-it", resultSet.getString(2));
                    return;
                }
            } catch (SQLException e) {
                lastQueryError = e;
            }
            Thread.sleep(2000);
        }
        AssertionError error =
                new AssertionError(
                        "Timed out waiting for TLS Stream Load; connector state=" + lastState);
        if (lastQueryError != null) {
            error.initCause(lastQueryError);
        }
        throw error;
    }

    private static void executeSql(String sql) {
        try (Connection connection = dorisService.getQueryConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new AssertionError("Failed to execute Doris SQL: " + sql, e);
        }
    }
}
