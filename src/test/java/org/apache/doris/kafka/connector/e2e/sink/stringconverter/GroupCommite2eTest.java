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

package org.apache.doris.kafka.connector.e2e.sink.stringconverter;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.kafka.connector.cfg.DorisOptions;
import org.apache.doris.kafka.connector.cfg.DorisSinkConnectorConfig;
import org.apache.doris.kafka.connector.exception.DorisException;
import org.apache.doris.kafka.connector.utils.ConfigCheckUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class GroupCommite2eTest extends AbstractStringE2ESinkTest {
    private static String connectorName;
    private static String jsonMsgConnectorContent;
    private static DorisOptions dorisOptions;
    private static String database;

    @BeforeClass
    public static void setUp() {
        initServer();
        initProducer();
        initialize();
    }

    public static void initialize() {
        jsonMsgConnectorContent =
                loadContent("src/test/resources/e2e/string_converter/group_commit_connector.json");
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
    }

    @Test
    public void testGroupCommit() throws Exception {
        String topic = "group_commit_test";
        String msg1 = "{\"id\":1,\"name\":\"kafka\",\"age\":12}";
        String msg2 = "{\"id\":2,\"name\":\"doris\",\"age\":10}";

        produceMsg2Kafka(topic, msg1);
        produceMsg2Kafka(topic, msg2);
        String tableSql =
                loadContent("src/test/resources/e2e/string_converter/group_commit_tab.sql");
        createTable(tableSql);
        kafkaContainerService.registerKafkaConnector(connectorName, jsonMsgConnectorContent);
        Thread.sleep(25000);

        String table = dorisOptions.getTopicMapTable(topic);
        List<String> expected = Arrays.asList("1,kafka,12", "2,doris,10");
        String query = String.format("select id,name,age from %s.%s order by id", database, table);
        checkResult(expected, query, 3);
    }

    public void checkResult(List<String> expected, String query, int columnSize) throws Exception {
        List<String> actual = new ArrayList<>();

        try (Statement statement = getJdbcConnection().createStatement()) {
            ResultSet sinkResultSet = statement.executeQuery(query);
            while (sinkResultSet.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= columnSize; i++) {
                    Object value = sinkResultSet.getObject(i);
                    if (value == null) {
                        row.add("null");
                    } else {
                        row.add(value.toString());
                    }
                }
                actual.add(StringUtils.join(row, ","));
            }
        }
        Assert.assertArrayEquals(expected.toArray(), actual.toArray());
    }

    @AfterClass
    public static void closeInstance() {
        kafkaContainerService.deleteKafkaConnector(connectorName);
    }
}
