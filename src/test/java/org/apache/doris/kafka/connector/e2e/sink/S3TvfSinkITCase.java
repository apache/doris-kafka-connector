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

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.URI;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.kafka.connector.cfg.DorisSinkConnectorConfig;
import org.apache.doris.kafka.connector.e2e.sink.stringconverter.AbstractStringE2ESinkTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

public class S3TvfSinkITCase extends AbstractStringE2ESinkTest {
    private static final String MINIO_IMAGE = "quay.io/minio/minio:RELEASE.2024-10-13T13-34-11Z";
    private static final int MINIO_PORT = 9000;
    private static final String ACCESS_KEY = "minioadmin";
    private static final String SECRET_KEY = "minioadmin";
    private static final String REGION = "us-east-1";
    private static final String BUCKET = "doris-kafka-connector-it";
    private static final String DATABASE = "s3_tvf_it";
    private static final String JSON_TOPIC = "s3_tvf_json_topic";
    private static final String JSON_TABLE = "s3_tvf_json_table";
    private static final String JSON_CONNECTOR = "s3_tvf_json_connector";
    private static final String JSON_LABEL_PREFIX = "s3_tvf_json";
    private static final String JSON_S3_PREFIX = "it/json";
    private static final String DEBEZIUM_TOPIC = "s3_tvf_debezium_topic";
    private static final String DEBEZIUM_TABLE = "s3_tvf_debezium_table";
    private static final String DEBEZIUM_CONNECTOR = "s3_tvf_debezium_connector";
    private static final String DEBEZIUM_S3_PREFIX = "it/debezium";
    private static final long WAIT_TIMEOUT_NANOS = TimeUnit.MINUTES.toNanos(2);

    private static GenericContainer<?> minioContainer;
    private static S3Client s3Client;
    private static String s3Endpoint;

    private String registeredConnector;

    @BeforeClass
    public static void setUp() throws Exception {
        initServer();
        initProducer();
        startMinio();
        createDatabase(DATABASE);
    }

    @After
    public void unregisterConnector() {
        if (registeredConnector != null) {
            kafkaContainerService.deleteKafkaConnector(registeredConnector);
            registeredConnector = null;
        }
    }

    @AfterClass
    public static void stopMinio() {
        if (s3Client != null) {
            s3Client.close();
        }
        if (minioContainer != null) {
            minioContainer.close();
        }
    }

    @Test
    public void testJsonRecordsCreateMultipleTvfFiles() throws Exception {
        resetTable(JSON_TABLE, false);
        produceMsg2Kafka(JSON_TOPIC, "{\"id\":1,\"name\":\"alice\",\"age\":20}");
        produceMsg2Kafka(JSON_TOPIC, "{\"id\":2,\"name\":\"bob\",\"age\":30}");
        produceMsg2Kafka(JSON_TOPIC, "{\"id\":3,\"name\":\"carol\",\"age\":40}");

        registerConnector(
                JSON_CONNECTOR,
                connectorConfig(
                        JSON_CONNECTOR,
                        JSON_TOPIC,
                        JSON_TABLE,
                        JSON_LABEL_PREFIX,
                        JSON_S3_PREFIX,
                        "org.apache.kafka.connect.storage.StringConverter",
                        "normal",
                        false));

        String query =
                String.format("SELECT id,name,age FROM %s.%s ORDER BY id", DATABASE, JSON_TABLE);
        waitForRows(
                JSON_CONNECTOR, query, Arrays.asList("1,alice,20", "2,bob,30", "3,carol,40"), 3);

        List<String> objectKeys = waitForObjectCount(JSON_CONNECTOR, JSON_S3_PREFIX + "/", 3);
        assertJsonObjectNames(objectKeys);
    }

    @Test
    public void testDebeziumInsertUpdateDeleteThroughTvf() throws Exception {
        resetTable(DEBEZIUM_TABLE, true);
        registerConnector(
                DEBEZIUM_CONNECTOR,
                connectorConfig(
                        DEBEZIUM_CONNECTOR,
                        DEBEZIUM_TOPIC,
                        DEBEZIUM_TABLE,
                        "s3_tvf_debezium",
                        DEBEZIUM_S3_PREFIX,
                        "org.apache.kafka.connect.json.JsonConverter",
                        "debezium_ingestion",
                        true));

        String query =
                String.format(
                        "SELECT id,name,age FROM %s.%s ORDER BY id", DATABASE, DEBEZIUM_TABLE);
        produceMsg2Kafka(DEBEZIUM_TOPIC, debeziumEvent(null, row(1, "alice", 20), "c"));
        waitForRows(DEBEZIUM_CONNECTOR, query, Collections.singletonList("1,alice,20"), 3);

        produceMsg2Kafka(
                DEBEZIUM_TOPIC, debeziumEvent(row(1, "alice", 20), row(1, "alice", 21), "u"));
        waitForRows(DEBEZIUM_CONNECTOR, query, Collections.singletonList("1,alice,21"), 3);

        produceMsg2Kafka(DEBEZIUM_TOPIC, debeziumEvent(row(1, "alice", 21), null, "d"));
        waitForRows(DEBEZIUM_CONNECTOR, query, Collections.emptyList(), 3);
    }

    private static void startMinio() throws Exception {
        minioContainer =
                new GenericContainer<>(DockerImageName.parse(MINIO_IMAGE))
                        .withEnv("MINIO_ROOT_USER", ACCESS_KEY)
                        .withEnv("MINIO_ROOT_PASSWORD", SECRET_KEY)
                        .withCommand("server", "/data", "--address", ":" + MINIO_PORT)
                        .withExposedPorts(MINIO_PORT)
                        .waitingFor(
                                Wait.forHttp("/minio/health/live")
                                        .forPort(MINIO_PORT)
                                        .withStartupTimeout(Duration.ofMinutes(2)));
        minioContainer.start();

        s3Endpoint =
                "http://" + resolveEndpointHost() + ":" + minioContainer.getMappedPort(MINIO_PORT);
        s3Client = createS3Client(s3Endpoint);
        s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
    }

    private static S3Client createS3Client(String endpoint) {
        return S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.of(REGION))
                .credentialsProvider(
                        StaticCredentialsProvider.create(
                                AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .serviceConfiguration(
                        S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .build();
    }

    private static String resolveEndpointHost() throws Exception {
        String dockerHost = DockerClientFactory.instance().dockerHostIpAddress();
        for (InetAddress address : InetAddress.getAllByName(dockerHost)) {
            if (isUsableEndpointAddress(address)) {
                return address.getHostAddress();
            }
        }

        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            NetworkInterface networkInterface = interfaces.nextElement();
            if (!networkInterface.isUp() || networkInterface.isLoopback()) {
                continue;
            }
            Enumeration<InetAddress> addresses = networkInterface.getInetAddresses();
            while (addresses.hasMoreElements()) {
                InetAddress address = addresses.nextElement();
                if (isUsableEndpointAddress(address)) {
                    return address.getHostAddress();
                }
            }
        }
        throw new IllegalStateException("Unable to resolve a non-loopback IPv4 S3 endpoint");
    }

    private static boolean isUsableEndpointAddress(InetAddress address) {
        return address instanceof Inet4Address
                && !address.isAnyLocalAddress()
                && !address.isLoopbackAddress();
    }

    private void resetTable(String table, boolean uniqueKey) {
        executeSql("DROP TABLE IF EXISTS " + DATABASE + "." + table);
        String keyModel = uniqueKey ? "UNIQUE KEY(id)" : "DUPLICATE KEY(id)";
        String tableProperties =
                uniqueKey
                        ? "PROPERTIES (\"replication_num\" = \"1\", "
                                + "\"enable_unique_key_merge_on_write\" = \"true\")"
                        : "PROPERTIES (\"replication_num\" = \"1\")";
        createTable(
                "CREATE TABLE "
                        + DATABASE
                        + "."
                        + table
                        + " (id INT, name VARCHAR(64), age INT) "
                        + keyModel
                        + " DISTRIBUTED BY HASH(id) BUCKETS 1 "
                        + tableProperties);
    }

    private String connectorConfig(
            String connectorName,
            String topic,
            String table,
            String labelPrefix,
            String s3Prefix,
            String valueConverter,
            String converterMode,
            boolean enableDelete)
            throws Exception {
        ObjectNode root = objectMapper.createObjectNode();
        root.put(NAME, connectorName);
        ObjectNode config = root.putObject(CONFIG);
        config.put("connector.class", "org.apache.doris.kafka.connector.DorisSinkConnector");
        config.put("topics", topic);
        config.put("tasks.max", "1");
        config.put("doris.topic2table.map", topic + ":" + table);
        config.put("buffer.count.records", "100");
        config.put("buffer.flush.time", "1");
        config.put("buffer.size.bytes", "1");
        config.put("label.prefix", labelPrefix);
        config.put(DorisSinkConnectorConfig.DORIS_URLS, "127.0.0.1");
        config.put(DorisSinkConnectorConfig.DORIS_HTTP_PORT, "8030");
        config.put(DorisSinkConnectorConfig.DORIS_QUERY_PORT, "9030");
        config.put(DorisSinkConnectorConfig.DORIS_USER, "root");
        config.put(DorisSinkConnectorConfig.DORIS_PASSWORD, "");
        config.put(DorisSinkConnectorConfig.DORIS_DATABASE, DATABASE);
        config.put("load.model", "tvf");
        config.put("enable.combine.flush", "true");
        config.put("delivery.guarantee", "at_least_once");
        config.put("enable.delete", Boolean.toString(enableDelete));
        config.put("converter.mode", converterMode);
        config.put("key.converter", valueConverter);
        config.put("value.converter", valueConverter);
        if ("org.apache.kafka.connect.json.JsonConverter".equals(valueConverter)) {
            config.put("value.converter.schemas.enable", "true");
        }
        config.put("sink.properties.columns", "id,name,age");
        config.put("sink.s3.endpoint", s3Endpoint);
        config.put("sink.s3.region", REGION);
        config.put("sink.s3.bucket", BUCKET);
        config.put("sink.s3.prefix", s3Prefix);
        config.put("sink.s3.access-key", ACCESS_KEY);
        config.put("sink.s3.secret-key", SECRET_KEY);
        config.put("sink.s3.path-style-access", "true");
        return configureDorisConnector(objectMapper.writeValueAsString(root));
    }

    private void registerConnector(String name, String config) throws Exception {
        registeredConnector = name;
        kafkaContainerService.registerKafkaConnector(name, config);
    }

    private static ObjectNode row(int id, String name, int age) {
        ObjectNode row = objectMapper.createObjectNode();
        row.put("id", id);
        row.put("name", name);
        row.put("age", age);
        return row;
    }

    private static String debeziumEvent(ObjectNode before, ObjectNode after, String operation)
            throws Exception {
        ObjectNode rowSchema = objectMapper.createObjectNode();
        rowSchema.put("type", "struct");
        rowSchema.put("optional", true);
        rowSchema.put("name", "s3.tvf.it.Customer.Value");
        ArrayNode rowFields = rowSchema.putArray("fields");
        rowFields.add(fieldSchema("int32", false, "id"));
        rowFields.add(fieldSchema("string", false, "name"));
        rowFields.add(fieldSchema("int32", false, "age"));

        ObjectNode envelopeSchema = objectMapper.createObjectNode();
        envelopeSchema.put("type", "struct");
        envelopeSchema.put("optional", false);
        envelopeSchema.put("name", "s3.tvf.it.Customer.Envelope");
        ArrayNode envelopeFields = envelopeSchema.putArray("fields");
        ObjectNode beforeSchema = rowSchema.deepCopy();
        beforeSchema.put("field", "before");
        envelopeFields.add(beforeSchema);
        ObjectNode afterSchema = rowSchema.deepCopy();
        afterSchema.put("field", "after");
        envelopeFields.add(afterSchema);
        envelopeFields.add(fieldSchema("string", false, "op"));

        ObjectNode payload = objectMapper.createObjectNode();
        if (before == null) {
            payload.putNull("before");
        } else {
            payload.set("before", before);
        }
        if (after == null) {
            payload.putNull("after");
        } else {
            payload.set("after", after);
        }
        payload.put("op", operation);

        ObjectNode event = objectMapper.createObjectNode();
        event.set("schema", envelopeSchema);
        event.set("payload", payload);
        return objectMapper.writeValueAsString(event);
    }

    private static ObjectNode fieldSchema(String type, boolean optional, String field) {
        ObjectNode schema = objectMapper.createObjectNode();
        schema.put("type", type);
        schema.put("optional", optional);
        schema.put("field", field);
        return schema;
    }

    private void waitForRows(
            String connectorName, String query, List<String> expected, int columnCount)
            throws Exception {
        long deadline = System.nanoTime() + WAIT_TIMEOUT_NANOS;
        List<String> actual = Collections.emptyList();
        SQLException lastError = null;
        while (System.nanoTime() < deadline) {
            assertConnectorIsHealthy(connectorName);
            try {
                actual = queryRows(query, columnCount);
                lastError = null;
                if (expected.equals(actual)) {
                    return;
                }
            } catch (SQLException e) {
                lastError = e;
            }
            Thread.sleep(2000);
        }
        AssertionError error =
                new AssertionError(
                        "Timed out waiting for Doris rows. expected="
                                + expected
                                + ", actual="
                                + actual);
        if (lastError != null) {
            error.initCause(lastError);
        }
        throw error;
    }

    private static List<String> queryRows(String query, int columnCount) throws SQLException {
        List<String> rows = new ArrayList<>();
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(query)) {
            while (resultSet.next()) {
                List<String> values = new ArrayList<>();
                for (int index = 1; index <= columnCount; index++) {
                    Object value = resultSet.getObject(index);
                    values.add(value == null ? "null" : value.toString());
                }
                rows.add(StringUtils.join(values, ","));
            }
        }
        return rows;
    }

    private List<String> waitForObjectCount(String connectorName, String prefix, int expectedCount)
            throws Exception {
        long deadline = System.nanoTime() + WAIT_TIMEOUT_NANOS;
        List<String> objectKeys = Collections.emptyList();
        while (System.nanoTime() < deadline) {
            assertConnectorIsHealthy(connectorName);
            objectKeys = listObjectKeys(prefix);
            if (objectKeys.size() >= expectedCount) {
                Assert.assertEquals(expectedCount, objectKeys.size());
                return objectKeys;
            }
            Thread.sleep(2000);
        }
        throw new AssertionError(
                "Timed out waiting for "
                        + expectedCount
                        + " S3 objects under "
                        + prefix
                        + "; found "
                        + objectKeys);
    }

    private static List<String> listObjectKeys(String prefix) {
        return s3Client
                .listObjectsV2(ListObjectsV2Request.builder().bucket(BUCKET).prefix(prefix).build())
                .contents().stream()
                .map(object -> object.key())
                .sorted()
                .collect(Collectors.toList());
    }

    private void assertConnectorIsHealthy(String connectorName) {
        String state = kafkaContainerService.getConnectorTaskStatus(connectorName);
        Assert.assertFalse(
                "Kafka Connect task failed for " + connectorName, "FAILED".equalsIgnoreCase(state));
    }

    private static void assertJsonObjectNames(List<String> objectKeys) {
        Pattern fileNamePattern =
                Pattern.compile(
                        "^"
                                + Pattern.quote(
                                        JSON_LABEL_PREFIX + "_" + DATABASE + "_" + JSON_TABLE + "_")
                                + "([0-9a-f]{32})_0_([0-9]+)\\.json\\.gz$");
        Set<String> batchUuids = new HashSet<>();
        Set<String> fileNumbers = new HashSet<>();
        for (String objectKey : objectKeys) {
            String fileName = objectKey.substring(objectKey.lastIndexOf('/') + 1);
            Matcher matcher = fileNamePattern.matcher(fileName);
            Assert.assertTrue("Unexpected S3 TVF object name: " + objectKey, matcher.matches());
            batchUuids.add(matcher.group(1));
            fileNumbers.add(matcher.group(2));
        }
        Assert.assertEquals(1, batchUuids.size());
        Assert.assertEquals(new HashSet<>(Arrays.asList("0", "1", "2")), fileNumbers);
    }
}
