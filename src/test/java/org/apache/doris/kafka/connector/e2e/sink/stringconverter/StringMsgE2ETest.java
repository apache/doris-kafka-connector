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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import org.apache.doris.kafka.connector.cfg.DorisOptions;
import org.apache.doris.kafka.connector.cfg.DorisSinkConnectorConfig;
import org.apache.doris.kafka.connector.exception.DorisException;
import org.apache.doris.kafka.connector.utils.ConfigCheckUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class StringMsgE2ETest extends AbstractStringE2ESinkTest {
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
                loadContent("src/test/resources/e2e/string_converter/string_msg_connector.json");
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
    public void testStringMsg() throws IOException, InterruptedException, SQLException {
        String topic = "string_test";
        String msg = "{\"id\":1,\"name\":\"zhangsan\",\"age\":12}";

        produceMsg2Kafka(topic, msg);
        String tableSql = loadContent("src/test/resources/e2e/string_converter/string_msg_tab.sql");
        createTable(tableSql);
        kafkaContainerService.registerKafkaConnector(connectorName, jsonMsgConnectorContent);

        String table = dorisOptions.getTopicMapTable(topic);
        Statement statement = getJdbcConnection().createStatement();
        ResultSet resultSet = statement.executeQuery("select * from " + database + "." + table);
        if (resultSet.next()) {
            Assert.assertEquals(1, resultSet.getString("id"));
            Assert.assertEquals("zhangsan", resultSet.getString("name"));
            Assert.assertEquals(12, resultSet.getString("12"));
        }
    }

    @AfterClass
    public static void closeInstance() {
        kafkaContainerService.deleteKafkaConnector(connectorName);
    }
}
