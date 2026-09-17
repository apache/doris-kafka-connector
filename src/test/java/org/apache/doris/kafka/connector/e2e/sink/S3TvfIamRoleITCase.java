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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.doris.kafka.connector.e2e.doris.DorisCustomerServiceImpl;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

/** Opt-in integration test for Kafka Connect S3 TVF writes with an AWS IAM role. */
public class S3TvfIamRoleITCase {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String DATABASE = "test_s3_tvf_iam_role";
    private static DorisCustomerServiceImpl doris;

    @BeforeClass
    public static void useExternalEnvironment() {
        Assume.assumeTrue(Boolean.getBoolean("s3_tvf_iam_role_it"));
        Assume.assumeTrue(Boolean.getBoolean("customer_env"));
        requiredProperty("kafka_bootstrap_servers");
        requiredProperty("kafka_connect_url");
        requiredProperty("s3_endpoint");
        requiredProperty("s3_region");
        requiredProperty("s3_bucket");
        requiredProperty("s3_role_arn");
        doris = new DorisCustomerServiceImpl();
        doris.startContainer();
    }

    @Test
    public void testWritesThroughIamRole() throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String connector = "s3-tvf-iam-role-" + suffix;
        String topic = "s3-tvf-iam-role-" + suffix;
        String table = "iam_role_" + suffix;
        boolean registered = false;
        try {
            executeSql(
                    "CREATE DATABASE IF NOT EXISTS `" + DATABASE + "`",
                    "CREATE TABLE `"
                            + DATABASE
                            + "`.`"
                            + table
                            + "` (`id` INT, `name` VARCHAR(64)) "
                            + "DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1 "
                            + "PROPERTIES (\"replication_num\" = \"1\")");
            registerConnector(connector, topic, table, suffix);
            registered = true;
            produce(topic, "{\"id\":1,\"name\":\"kafka\"}");
            waitForRow(table);
        } finally {
            if (registered) {
                deleteConnector(connector);
            }
            executeSql("DROP TABLE IF EXISTS `" + DATABASE + "`.`" + table + "`");
        }
    }

    private static void registerConnector(
            String connector, String topic, String table, String suffix) throws Exception {
        Properties properties = new Properties();
        try (InputStream stream =
                S3TvfIamRoleITCase.class
                        .getClassLoader()
                        .getResourceAsStream("s3-tvf-iam-role-sink.properties")) {
            properties.load(stream);
        }
        properties.put("topics", topic);
        properties.put("doris.topic2table.map", topic + ":" + table);
        properties.put("label.prefix", "iam_role_" + suffix);
        properties.put("doris.urls", requiredProperty("doris_host"));
        properties.put("doris.http.port", requiredProperty("doris_http_port"));
        properties.put("doris.query.port", requiredProperty("doris_query_port"));
        properties.put("doris.user", requiredProperty("doris_user"));
        properties.put("doris.password", System.getProperty("doris_passwd", ""));
        properties.put("doris.database", DATABASE);
        properties.put("sink.s3.endpoint", requiredProperty("s3_endpoint"));
        properties.put("sink.s3.region", requiredProperty("s3_region"));
        properties.put("sink.s3.bucket", requiredProperty("s3_bucket"));
        properties.put(
                "sink.s3.prefix", System.getProperty("s3_prefix", "doris-kafka-connector-it"));
        properties.put("sink.s3.role-arn", requiredProperty("s3_role_arn"));
        optionalProperty("s3_external_id")
                .ifPresent(value -> properties.put("sink.s3.external-id", value));

        ObjectNode root = OBJECT_MAPPER.createObjectNode();
        root.put("name", connector);
        ObjectNode config = root.putObject("config");
        properties.forEach((key, value) -> config.put(key.toString(), value.toString()));
        request("POST", "/connectors", OBJECT_MAPPER.writeValueAsBytes(root), 201);
    }

    private static void produce(String topic, String value) throws Exception {
        Properties properties = new Properties();
        properties.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                requiredProperty("kafka_bootstrap_servers"));
        properties.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            producer.send(new ProducerRecord<>(topic, value)).get(30, TimeUnit.SECONDS);
        }
    }

    private static void waitForRow(String table) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(2);
        while (System.nanoTime() < deadline) {
            try (Connection connection = doris.getQueryConnection();
                    Statement statement = connection.createStatement();
                    ResultSet result =
                            statement.executeQuery(
                                    "SELECT id,name FROM `" + DATABASE + "`.`" + table + "`")) {
                if (result.next()) {
                    Assert.assertEquals(1, result.getInt(1));
                    Assert.assertEquals("kafka", result.getString(2));
                    return;
                }
            }
            Thread.sleep(1000);
        }
        Assert.fail("Timed out waiting for Kafka Connect IAM role row");
    }

    private static void executeSql(String... sql) throws Exception {
        try (Connection connection = doris.getQueryConnection();
                Statement statement = connection.createStatement()) {
            for (String value : sql) {
                statement.execute(value);
            }
        }
    }

    private static void deleteConnector(String connector) throws Exception {
        request("DELETE", "/connectors/" + connector, null, 204);
    }

    private static void request(String method, String path, byte[] body, int expected)
            throws Exception {
        HttpURLConnection connection =
                (HttpURLConnection)
                        new URL(requiredProperty("kafka_connect_url") + path).openConnection();
        connection.setRequestMethod(method);
        connection.setConnectTimeout(10000);
        connection.setReadTimeout(30000);
        if (body != null) {
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type", "application/json");
            try (OutputStream output = connection.getOutputStream()) {
                output.write(body);
            }
        }
        int status = connection.getResponseCode();
        if (status != expected) {
            InputStream response =
                    status >= 400 ? connection.getErrorStream() : connection.getInputStream();
            String message =
                    response == null ? "" : new String(readAll(response), StandardCharsets.UTF_8);
            throw new IllegalStateException("Kafka Connect returned " + status + ": " + message);
        }
        connection.disconnect();
    }

    private static byte[] readAll(InputStream input) throws Exception {
        byte[] buffer = new byte[1024];
        java.io.ByteArrayOutputStream output = new java.io.ByteArrayOutputStream();
        try (InputStream stream = input) {
            int length;
            while ((length = stream.read(buffer)) != -1) {
                output.write(buffer, 0, length);
            }
        }
        return output.toByteArray();
    }

    private static String requiredProperty(String name) {
        return optionalProperty(name)
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        "Missing required system property: " + name));
    }

    private static java.util.Optional<String> optionalProperty(String name) {
        String value = System.getProperty(name);
        return value == null || value.trim().isEmpty()
                ? java.util.Optional.empty()
                : java.util.Optional.of(value.trim());
    }
}
