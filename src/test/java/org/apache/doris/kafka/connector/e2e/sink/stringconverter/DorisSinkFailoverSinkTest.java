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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import org.apache.doris.kafka.connector.cfg.DorisOptions;
import org.apache.doris.kafka.connector.cfg.DorisSinkConnectorConfig;
import org.apache.doris.kafka.connector.exception.DorisException;
import org.apache.doris.kafka.connector.utils.ConfigCheckUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** DorisSinkFailoverSinkTest is a test class for Doris Sink Connector. */
@RunWith(Parameterized.class)
@Ignore
public class DorisSinkFailoverSinkTest extends AbstractStringE2ESinkTest {
    private static final Logger LOG = LoggerFactory.getLogger(DorisSinkFailoverSinkTest.class);
    private static String connectorName;
    private static String jsonMsgConnectorContent;
    private static DorisOptions dorisOptions;
    private static String database;
    private boolean enableCombineFlush;

    public DorisSinkFailoverSinkTest(boolean enableCombineFlush) {
        this.enableCombineFlush = enableCombineFlush;
    }

    @BeforeClass
    public static void setUp() {
        initServer();
        initProducer();
    }

    @Parameterized.Parameters(name = "enableCombineFlush: {0}")
    public static Object[] parameters() {
        return new Object[][] {new Object[] {true}, new Object[] {true}};
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

    /** mock streamload failure */
    @Test
    public void testStreamLoadFailoverSink() throws Exception {
        LOG.info("start to test testStreamLoadFailoverSink.");
        initialize("src/test/resources/e2e/string_converter/string_msg_failover_connector.json");

        // replace file path
        String connectJson =
                loadContent(
                        "src/test/resources/e2e/string_converter/string_msg_failover_connector.json");
        JsonNode jsonNode = new ObjectMapper().readTree(connectJson);
        ObjectNode configNode = (ObjectNode) jsonNode.get("config");

        configNode.put(DorisSinkConnectorConfig.ENABLE_COMBINE_FLUSH, enableCombineFlush);
        jsonMsgConnectorContent = new ObjectMapper().writeValueAsString(jsonNode);

        Thread.sleep(5000);
        String topic = "string_test_failover";
        String msg1 = "{\"id\":1,\"name\":\"zhangsan\",\"age\":12}";
        produceMsg2Kafka(topic, msg1);

        String tableSql =
                loadContent("src/test/resources/e2e/string_converter/string_msg_tab_failover.sql");
        createTable(tableSql);

        kafkaContainerService.registerKafkaConnector(connectorName, jsonMsgConnectorContent);

        String table = dorisOptions.getTopicMapTable(topic);
        String querySql =
                String.format("select id,name,age from %s.%s order by id", database, table);
        LOG.info("start to query result from doris. sql={}", querySql);
        while (true) {
            List<String> result = executeSQLStatement(getJdbcConnection(), LOG, querySql, 3);
            // until load success one time
            if (result.size() >= 1) {
                faultInjectionOpen();
                // mock new data
                String msg2 = "{\"id\":2,\"name\":\"lisi\",\"age\":18}";
                produceMsg2Kafka(topic, msg2);
                Thread.sleep(15000);
                faultInjectionClear();
                break;
            } else {
                Thread.sleep(1000);
            }
        }

        String msg3 = "{\"id\":3,\"name\":\"wangwu\",\"age\":38}";
        produceMsg2Kafka(topic, msg3);
        Thread.sleep(25000);

        List<String> excepted = Arrays.asList("1,zhangsan,12", "2,lisi,18", "3,wangwu,38");
        checkResult(excepted, querySql, 3);
    }

    public static List<String> executeSQLStatement(
            Connection connection, Logger logger, String sql, int columnSize) {
        List<String> result = new ArrayList<>();
        if (Objects.isNull(sql)) {
            return result;
        }
        try (Statement statement = connection.createStatement()) {
            logger.info("start to execute sql={}", sql);
            ResultSet resultSet = statement.executeQuery(sql);

            while (resultSet.next()) {
                StringJoiner sb = new StringJoiner(",");
                for (int i = 1; i <= columnSize; i++) {
                    Object value = resultSet.getObject(i);
                    sb.add(String.valueOf(value));
                }
                result.add(sb.toString());
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
