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

package org.apache.doris.kafka.connector.e2e.sink.avro;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.doris.kafka.connector.cfg.DorisOptions;
import org.apache.doris.kafka.connector.cfg.DorisSinkConnectorConfig;
import org.apache.doris.kafka.connector.exception.DorisException;
import org.apache.doris.kafka.connector.utils.ConfigCheckUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class AvroMsgE2ETest extends AbstractAvroE2ESinkTest {
    private static String connectorName;
    private static String jsonMsgConnectorContent;
    private static DorisOptions dorisOptions;
    private static String database;

    @BeforeClass
    public static void setUp() {
        initServer();
        initSchemaRegistry();
    }

    public static void initialize(String connectorPath) {
        jsonMsgConnectorContent = loadContent(connectorPath);
        JsonNode rootNode = null;
        try {
            rootNode = objectMapper.readTree(jsonMsgConnectorContent);
        } catch (IOException e) {
            throw new DorisException("Failed to read content body.", e);
        }
        connectorName = rootNode.get(NAME).asText();
        JsonNode configNode = rootNode.get(CONFIG);
        Map<String, String> configMap = objectMapper.convertValue(configNode, Map.class);
        configMap.put(ConfigCheckUtils.TASK_ID, "1");
        Map<String, String> lowerCaseConfigMap =
                DorisSinkConnectorConfig.convertToLowercase(configMap);
        DorisSinkConnectorConfig.setDefaultValues(lowerCaseConfigMap);
        dorisOptions = new DorisOptions(lowerCaseConfigMap);
        database = dorisOptions.getDatabase();
        createDatabase(database);
        setTimeZone();
    }

    private static void setTimeZone() {
        executeSql(getJdbcConnection(), "set global time_zone = 'Asia/Shanghai'");
    }

    @Test
    public void testConfluentAvroConvert() throws Exception {
        initAvroProducer();
        initialize("src/test/resources/e2e/avro_converter/confluent_avro_convert.json");

        // replace file path
        String connectJson =
                loadContent("src/test/resources/e2e/avro_converter/confluent_avro_convert.json");
        JsonNode jsonNode = new ObjectMapper().readTree(connectJson);
        ObjectNode configNode = (ObjectNode) jsonNode.get("config");

        configNode.put("key.converter.schema.registry.url", getSchemaRegistryUrl());
        configNode.put("value.converter.schema.registry.url", getSchemaRegistryUrl());
        jsonMsgConnectorContent = new ObjectMapper().writeValueAsString(jsonNode);
        Thread.sleep(5000);

        String topic = "avro-user";
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(loadContent("src/test/resources/decode/avro/user.avsc"));

        GenericRecord user1 = new GenericData.Record(schema);
        user1.put("id", 3);
        user1.put("name", "kafka-confluent");
        user1.put("age", 38);
        produceMsg2Kafka(topic, user1);

        GenericRecord user2 = new GenericData.Record(schema);
        user2.put("id", 4);
        user2.put("name", "doris-confluent");
        user2.put("age", 58);
        produceMsg2Kafka(topic, user2);

        String tableSql =
                loadContent("src/test/resources/e2e/avro_converter/confluent_avro_tab.sql");
        createTable(tableSql);
        kafkaContainerService.registerKafkaConnector(connectorName, jsonMsgConnectorContent);
        Thread.sleep(25000);

        String table = dorisOptions.getTopicMapTable(topic);
        List<String> expected = Arrays.asList("3,kafka-confluent,38", "4,doris-confluent,58");
        String query = String.format("select id,name,age from %s.%s order by id", database, table);
        checkResult(expected, query, 3);
    }

    @Test
    public void testDorisAvroConvert() throws Exception {
        initByteProducer();
        initialize("src/test/resources/e2e/avro_converter/doris_avro_convert.json");
        // replace file path
        String connectJson =
                loadContent("src/test/resources/e2e/avro_converter/doris_avro_convert.json");
        JsonNode jsonNode = new ObjectMapper().readTree(connectJson);
        ObjectNode configNode = (ObjectNode) jsonNode.get("config");

        String absolutePath = getAbsolutePath("decode/avro/user.avsc");
        configNode.put(
                "value.converter.avro.topic2schema.filepath", "avro-user:file://" + absolutePath);
        jsonMsgConnectorContent = new ObjectMapper().writeValueAsString(jsonNode);

        Thread.sleep(5000);

        String topic = "avro-user";
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(loadContent("src/test/resources/decode/avro/user.avsc"));

        GenericRecord user1 = new GenericData.Record(schema);
        user1.put("id", 1);
        user1.put("name", "kafka");
        user1.put("age", 30);
        produceMsg2Kafka(topic, convertAvro2Byte(user1, schema));

        GenericRecord user2 = new GenericData.Record(schema);
        user2.put("id", 2);
        user2.put("name", "doris");
        user2.put("age", 18);
        produceMsg2Kafka(topic, convertAvro2Byte(user2, schema));

        String tableSql = loadContent("src/test/resources/e2e/avro_converter/doris_avro_tab.sql");
        createTable(tableSql);
        kafkaContainerService.registerKafkaConnector(connectorName, jsonMsgConnectorContent);
        Thread.sleep(25000);

        String table = dorisOptions.getTopicMapTable(topic);
        List<String> expected = Arrays.asList("1,kafka,30", "2,doris,18");
        String query = String.format("select id,name,age from %s.%s order by id", database, table);
        checkResult(expected, query, 3);
    }

    @AfterClass
    public static void closeInstance() {
        kafkaContainerService.deleteKafkaConnector(connectorName);
    }

    public static String getAbsolutePath(String fileName) {
        ClassLoader classLoader = AvroMsgE2ETest.class.getClassLoader();
        URL resource = classLoader.getResource(fileName);
        if (resource != null) {
            return Paths.get(resource.getPath()).toAbsolutePath().toString();
        } else {
            return null;
        }
    }

    public static byte[] convertAvro2Byte(GenericRecord data, Schema schema) throws IOException {
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(data, encoder);
        encoder.flush();
        byte[] avroBytes = out.toByteArray();
        return avroBytes;
    }
}
