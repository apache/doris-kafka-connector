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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StringMsgE2ETest extends AbstractStringE2ESinkTest {
    private static final Logger LOG = LoggerFactory.getLogger(StringMsgE2ETest.class);
    private static String connectorName;
    private static String jsonMsgConnectorContent;
    private static DorisOptions dorisOptions;
    private static String database;

    @BeforeClass
    public static void setUp() {
        initServer();
        initProducer();
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
    public void testStringMsg() throws IOException, InterruptedException, SQLException {
        initialize("src/test/resources/e2e/string_converter/string_msg_connector.json");
        Thread.sleep(5000);
        String topic = "string_test";
        String msg = "{\"id\":1,\"name\":\"zhangsan\",\"age\":12}";

        produceMsg2Kafka(topic, msg);
        String tableSql = loadContent("src/test/resources/e2e/string_converter/string_msg_tab.sql");
        createTable(tableSql);
        kafkaContainerService.registerKafkaConnector(connectorName, jsonMsgConnectorContent);

        String table = dorisOptions.getTopicMapTable(topic);
        Statement statement = getJdbcConnection().createStatement();
        String querySql = "select * from " + database + "." + table;
        LOG.info("start to query result from doris. sql={}", querySql);
        ResultSet resultSet = statement.executeQuery(querySql);

        Assert.assertTrue(resultSet.next());

        int id = resultSet.getInt("id");
        String name = resultSet.getString("name");
        int age = resultSet.getInt("age");
        LOG.info("Query result is id={}, name={}, age={}", id, name, age);

        Assert.assertEquals(1, id);
        Assert.assertEquals("zhangsan", name);
        Assert.assertEquals(12, age);
    }

    @Test
    public void testGroupCommit() throws Exception {

        initialize("src/test/resources/e2e/string_converter/group_commit_connector.json");
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

    @Test
    public void testPartialUpdate() throws Exception {
        initialize("src/test/resources/e2e/string_converter/partial_update.json");
        String topic = "partial_update_test";
        String msg1 =
                "{\"id\":1,\"col1\":\"after_update_col1_1\",\"col2\":\"after_update_col2_1\"}";
        String msg2 =
                "{\"id\":2,\"col1\":\"after_update_col1_2\",\"col2\":\"after_update_col2_2\"}";

        produceMsg2Kafka(topic, msg1);
        produceMsg2Kafka(topic, msg2);

        String tableSql =
                loadContent("src/test/resources/e2e/string_converter/partial_update_tab.sql");
        String insertSql =
                loadContent(
                        "src/test/resources/e2e/string_converter/insert_partial_update_tab.sql");
        createTable(tableSql);
        Thread.sleep(2000);
        insertTable(insertSql);
        Thread.sleep(15000);
        kafkaContainerService.registerKafkaConnector(connectorName, jsonMsgConnectorContent);

        String table = dorisOptions.getTopicMapTable(topic);
        List<String> expected =
                Arrays.asList(
                        "1,after_update_col1_1,after_update_col2_1,before_update_col3_1",
                        "2,after_update_col1_2,after_update_col2_2,before_update_col3_2");
        Thread.sleep(10000);
        String query =
                String.format("select id,col1,col2,col3 from %s.%s order by id", database, table);
        checkResult(expected, query, 4);
    }

    @Test
    public void testDebeziumIngestionFullTypes() throws Exception {
        initialize("src/test/resources/e2e/string_converter/full_types.json");
        String topic = "full_types";
        String msg1 =
                "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"struct\",\"fields\":[{\"type\":\"int64\",\"optional\":false,\"field\":\"id\"},{\"type\":\"int16\",\"optional\":true,\"field\":\"tiny_c\"},{\"type\":\"int16\",\"optional\":true,\"field\":\"tiny_un_c\"},{\"type\":\"int16\",\"optional\":true,\"field\":\"tiny_un_z_c\"},{\"type\":\"int16\",\"optional\":true,\"field\":\"small_c\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"small_un_c\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"small_un_z_c\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"medium_c\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"medium_un_c\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"medium_un_z_c\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"int_c\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"int_un_c\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"int_un_z_c\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"int11_c\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"big_c\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"big_un_c\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"big_un_z_c\"},{\"type\":\"string\",\"optional\":true,\"field\":\"varchar_c\"},{\"type\":\"string\",\"optional\":true,\"field\":\"char_c\"},{\"type\":\"double\",\"optional\":true,\"field\":\"real_c\"},{\"type\":\"double\",\"optional\":true,\"field\":\"float_c\"},{\"type\":\"double\",\"optional\":true,\"field\":\"float_un_c\"},{\"type\":\"double\",\"optional\":true,\"field\":\"float_un_z_c\"},{\"type\":\"double\",\"optional\":true,\"field\":\"double_c\"},{\"type\":\"double\",\"optional\":true,\"field\":\"double_un_c\"},{\"type\":\"double\",\"optional\":true,\"field\":\"double_un_z_c\"},{\"type\":\"bytes\",\"optional\":true,\"name\":\"org.apache.kafka.connect.data.Decimal\",\"version\":1,\"parameters\":{\"scale\":\"4\",\"connect.decimal.precision\":\"8\"},\"field\":\"decimal_c\"},{\"type\":\"bytes\",\"optional\":true,\"name\":\"org.apache.kafka.connect.data.Decimal\",\"version\":1,\"parameters\":{\"scale\":\"4\",\"connect.decimal.precision\":\"8\"},\"field\":\"decimal_un_c\"},{\"type\":\"bytes\",\"optional\":true,\"name\":\"org.apache.kafka.connect.data.Decimal\",\"version\":1,\"parameters\":{\"scale\":\"4\",\"connect.decimal.precision\":\"8\"},\"field\":\"decimal_un_z_c\"},{\"type\":\"bytes\",\"optional\":true,\"name\":\"org.apache.kafka.connect.data.Decimal\",\"version\":1,\"parameters\":{\"scale\":\"0\",\"connect.decimal.precision\":\"6\"},\"field\":\"numeric_c\"},{\"type\":\"bytes\",\"optional\":true,\"name\":\"org.apache.kafka.connect.data.Decimal\",\"version\":1,\"parameters\":{\"scale\":\"1\",\"connect.decimal.precision\":\"65\"},\"field\":\"big_decimal_c\"},{\"type\":\"boolean\",\"optional\":true,\"field\":\"bit1_c\"},{\"type\":\"int16\",\"optional\":true,\"field\":\"tiny1_c\"},{\"type\":\"int16\",\"optional\":true,\"field\":\"boolean_c\"},{\"type\":\"int32\",\"optional\":true,\"name\":\"io.debezium.time.Date\",\"version\":1,\"field\":\"date_c\"},{\"type\":\"int64\",\"optional\":true,\"name\":\"io.debezium.time.MicroTime\",\"version\":1,\"field\":\"time_c\"},{\"type\":\"int64\",\"optional\":true,\"name\":\"io.debezium.time.Timestamp\",\"version\":1,\"field\":\"datetime_c\"},{\"type\":\"string\",\"optional\":false,\"name\":\"io.debezium.time.ZonedTimestamp\",\"version\":1,\"default\":\"1970-01-01T00:00:00Z\",\"field\":\"timestamp_c\"},{\"type\":\"string\",\"optional\":true,\"field\":\"text_c\"},{\"type\":\"int32\",\"optional\":true,\"name\":\"io.debezium.time.Year\",\"version\":1,\"field\":\"year_c\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.data.Enum\",\"version\":1,\"parameters\":{\"allowed\":\"red,white\"},\"default\":\"red\",\"field\":\"enum_c\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.data.EnumSet\",\"version\":1,\"parameters\":{\"allowed\":\"a,b\"},\"field\":\"set_c\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.data.Json\",\"version\":1,\"field\":\"json_c\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"double\",\"optional\":false,\"field\":\"x\"},{\"type\":\"double\",\"optional\":false,\"field\":\"y\"},{\"type\":\"bytes\",\"optional\":true,\"field\":\"wkb\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"srid\"}],\"optional\":true,\"name\":\"io.debezium.data.geometry.Point\",\"version\":1,\"doc\":\"Geometry (POINT)\",\"field\":\"point_c\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"bytes\",\"optional\":false,\"field\":\"wkb\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"srid\"}],\"optional\":true,\"name\":\"io.debezium.data.geometry.Geometry\",\"version\":1,\"doc\":\"Geometry\",\"field\":\"geometry_c\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"bytes\",\"optional\":false,\"field\":\"wkb\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"srid\"}],\"optional\":true,\"name\":\"io.debezium.data.geometry.Geometry\",\"version\":1,\"doc\":\"Geometry\",\"field\":\"linestring_c\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"bytes\",\"optional\":false,\"field\":\"wkb\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"srid\"}],\"optional\":true,\"name\":\"io.debezium.data.geometry.Geometry\",\"version\":1,\"doc\":\"Geometry\",\"field\":\"polygon_c\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"bytes\",\"optional\":false,\"field\":\"wkb\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"srid\"}],\"optional\":true,\"name\":\"io.debezium.data.geometry.Geometry\",\"version\":1,\"doc\":\"Geometry\",\"field\":\"multipoint_c\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"bytes\",\"optional\":false,\"field\":\"wkb\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"srid\"}],\"optional\":true,\"name\":\"io.debezium.data.geometry.Geometry\",\"version\":1,\"doc\":\"Geometry\",\"field\":\"multiline_c\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"bytes\",\"optional\":false,\"field\":\"wkb\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"srid\"}],\"optional\":true,\"name\":\"io.debezium.data.geometry.Geometry\",\"version\":1,\"doc\":\"Geometry\",\"field\":\"multipolygon_c\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"bytes\",\"optional\":false,\"field\":\"wkb\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"srid\"}],\"optional\":true,\"name\":\"io.debezium.data.geometry.Geometry\",\"version\":1,\"doc\":\"Geometry\",\"field\":\"geometrycollection_c\"}],\"optional\":true,\"name\":\"mysql_test.doris_test.full_types.Value\",\"field\":\"before\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"int64\",\"optional\":false,\"field\":\"id\"},{\"type\":\"int16\",\"optional\":true,\"field\":\"tiny_c\"},{\"type\":\"int16\",\"optional\":true,\"field\":\"tiny_un_c\"},{\"type\":\"int16\",\"optional\":true,\"field\":\"tiny_un_z_c\"},{\"type\":\"int16\",\"optional\":true,\"field\":\"small_c\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"small_un_c\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"small_un_z_c\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"medium_c\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"medium_un_c\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"medium_un_z_c\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"int_c\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"int_un_c\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"int_un_z_c\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"int11_c\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"big_c\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"big_un_c\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"big_un_z_c\"},{\"type\":\"string\",\"optional\":true,\"field\":\"varchar_c\"},{\"type\":\"string\",\"optional\":true,\"field\":\"char_c\"},{\"type\":\"double\",\"optional\":true,\"field\":\"real_c\"},{\"type\":\"double\",\"optional\":true,\"field\":\"float_c\"},{\"type\":\"double\",\"optional\":true,\"field\":\"float_un_c\"},{\"type\":\"double\",\"optional\":true,\"field\":\"float_un_z_c\"},{\"type\":\"double\",\"optional\":true,\"field\":\"double_c\"},{\"type\":\"double\",\"optional\":true,\"field\":\"double_un_c\"},{\"type\":\"double\",\"optional\":true,\"field\":\"double_un_z_c\"},{\"type\":\"bytes\",\"optional\":true,\"name\":\"org.apache.kafka.connect.data.Decimal\",\"version\":1,\"parameters\":{\"scale\":\"4\",\"connect.decimal.precision\":\"8\"},\"field\":\"decimal_c\"},{\"type\":\"bytes\",\"optional\":true,\"name\":\"org.apache.kafka.connect.data.Decimal\",\"version\":1,\"parameters\":{\"scale\":\"4\",\"connect.decimal.precision\":\"8\"},\"field\":\"decimal_un_c\"},{\"type\":\"bytes\",\"optional\":true,\"name\":\"org.apache.kafka.connect.data.Decimal\",\"version\":1,\"parameters\":{\"scale\":\"4\",\"connect.decimal.precision\":\"8\"},\"field\":\"decimal_un_z_c\"},{\"type\":\"bytes\",\"optional\":true,\"name\":\"org.apache.kafka.connect.data.Decimal\",\"version\":1,\"parameters\":{\"scale\":\"0\",\"connect.decimal.precision\":\"6\"},\"field\":\"numeric_c\"},{\"type\":\"bytes\",\"optional\":true,\"name\":\"org.apache.kafka.connect.data.Decimal\",\"version\":1,\"parameters\":{\"scale\":\"1\",\"connect.decimal.precision\":\"65\"},\"field\":\"big_decimal_c\"},{\"type\":\"boolean\",\"optional\":true,\"field\":\"bit1_c\"},{\"type\":\"int16\",\"optional\":true,\"field\":\"tiny1_c\"},{\"type\":\"int16\",\"optional\":true,\"field\":\"boolean_c\"},{\"type\":\"int32\",\"optional\":true,\"name\":\"io.debezium.time.Date\",\"version\":1,\"field\":\"date_c\"},{\"type\":\"int64\",\"optional\":true,\"name\":\"io.debezium.time.MicroTime\",\"version\":1,\"field\":\"time_c\"},{\"type\":\"int64\",\"optional\":true,\"name\":\"io.debezium.time.Timestamp\",\"version\":1,\"field\":\"datetime_c\"},{\"type\":\"string\",\"optional\":false,\"name\":\"io.debezium.time.ZonedTimestamp\",\"version\":1,\"default\":\"1970-01-01T00:00:00Z\",\"field\":\"timestamp_c\"},{\"type\":\"string\",\"optional\":true,\"field\":\"text_c\"},{\"type\":\"int32\",\"optional\":true,\"name\":\"io.debezium.time.Year\",\"version\":1,\"field\":\"year_c\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.data.Enum\",\"version\":1,\"parameters\":{\"allowed\":\"red,white\"},\"default\":\"red\",\"field\":\"enum_c\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.data.EnumSet\",\"version\":1,\"parameters\":{\"allowed\":\"a,b\"},\"field\":\"set_c\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.data.Json\",\"version\":1,\"field\":\"json_c\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"double\",\"optional\":false,\"field\":\"x\"},{\"type\":\"double\",\"optional\":false,\"field\":\"y\"},{\"type\":\"bytes\",\"optional\":true,\"field\":\"wkb\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"srid\"}],\"optional\":true,\"name\":\"io.debezium.data.geometry.Point\",\"version\":1,\"doc\":\"Geometry (POINT)\",\"field\":\"point_c\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"bytes\",\"optional\":false,\"field\":\"wkb\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"srid\"}],\"optional\":true,\"name\":\"io.debezium.data.geometry.Geometry\",\"version\":1,\"doc\":\"Geometry\",\"field\":\"geometry_c\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"bytes\",\"optional\":false,\"field\":\"wkb\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"srid\"}],\"optional\":true,\"name\":\"io.debezium.data.geometry.Geometry\",\"version\":1,\"doc\":\"Geometry\",\"field\":\"linestring_c\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"bytes\",\"optional\":false,\"field\":\"wkb\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"srid\"}],\"optional\":true,\"name\":\"io.debezium.data.geometry.Geometry\",\"version\":1,\"doc\":\"Geometry\",\"field\":\"polygon_c\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"bytes\",\"optional\":false,\"field\":\"wkb\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"srid\"}],\"optional\":true,\"name\":\"io.debezium.data.geometry.Geometry\",\"version\":1,\"doc\":\"Geometry\",\"field\":\"multipoint_c\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"bytes\",\"optional\":false,\"field\":\"wkb\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"srid\"}],\"optional\":true,\"name\":\"io.debezium.data.geometry.Geometry\",\"version\":1,\"doc\":\"Geometry\",\"field\":\"multiline_c\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"bytes\",\"optional\":false,\"field\":\"wkb\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"srid\"}],\"optional\":true,\"name\":\"io.debezium.data.geometry.Geometry\",\"version\":1,\"doc\":\"Geometry\",\"field\":\"multipolygon_c\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"bytes\",\"optional\":false,\"field\":\"wkb\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"srid\"}],\"optional\":true,\"name\":\"io.debezium.data.geometry.Geometry\",\"version\":1,\"doc\":\"Geometry\",\"field\":\"geometrycollection_c\"}],\"optional\":true,\"name\":\"mysql_test.doris_test.full_types.Value\",\"field\":\"after\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"version\"},{\"type\":\"string\",\"optional\":false,\"field\":\"connector\"},{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"ts_ms\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.data.Enum\",\"version\":1,\"parameters\":{\"allowed\":\"true,last,false,incremental\"},\"default\":\"false\",\"field\":\"snapshot\"},{\"type\":\"string\",\"optional\":false,\"field\":\"db\"},{\"type\":\"string\",\"optional\":true,\"field\":\"sequence\"},{\"type\":\"string\",\"optional\":true,\"field\":\"table\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"server_id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"gtid\"},{\"type\":\"string\",\"optional\":false,\"field\":\"file\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"pos\"},{\"type\":\"int32\",\"optional\":false,\"field\":\"row\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"thread\"},{\"type\":\"string\",\"optional\":true,\"field\":\"query\"}],\"optional\":false,\"name\":\"io.debezium.connector.mysql.Source\",\"field\":\"source\"},{\"type\":\"string\",\"optional\":false,\"field\":\"op\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_ms\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"id\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"total_order\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"data_collection_order\"}],\"optional\":true,\"field\":\"transaction\"}],\"optional\":false,\"name\":\"mysql_test.doris_test.full_types.Envelope\"},\"payload\":{\"before\":null,\"after\":{\"id\":1,\"tiny_c\":127,\"tiny_un_c\":255,\"tiny_un_z_c\":255,\"small_c\":32767,\"small_un_c\":65535,\"small_un_z_c\":65535,\"medium_c\":8388607,\"medium_un_c\":16777215,\"medium_un_z_c\":16777215,\"int_c\":2147483647,\"int_un_c\":4294967295,\"int_un_z_c\":4294967295,\"int11_c\":2147483647,\"big_c\":9223372036854775807,\"big_un_c\":-1,\"big_un_z_c\":-1,\"varchar_c\":\"Hello World\",\"char_c\":\"abc\",\"real_c\":123.102,\"float_c\":123.10199737548828,\"float_un_c\":123.10299682617188,\"float_un_z_c\":123.10399627685547,\"double_c\":404.4443,\"double_un_c\":404.4444,\"double_un_z_c\":404.4445,\"decimal_c\":\"EtaH\",\"decimal_un_c\":\"EtaI\",\"decimal_un_z_c\":\"EtaJ\",\"numeric_c\":\"AVo=\",\"big_decimal_c\":\"FJqkSQ==\",\"bit1_c\":false,\"tiny1_c\":1,\"boolean_c\":1,\"date_c\":18460,\"time_c\":64822000000,\"datetime_c\":1595008822000,\"timestamp_c\":\"2020-07-17T10:00:22Z\",\"text_c\":\"text\",\"year_c\":2021,\"enum_c\":\"red\",\"set_c\":\"a,b\",\"json_c\":\"{\\\"key1\\\":\\\"value1\\\"}\",\"point_c\":{\"x\":3.0,\"y\":1.0,\"wkb\":\"AQEAAAAAAAAAAAAIQAAAAAAAAPA/\",\"srid\":null},\"geometry_c\":{\"wkb\":\"AQMAAAABAAAABQAAAAAAAAAAAPA/AAAAAAAA8D8AAAAAAAAAQAAAAAAAAPA/AAAAAAAAAEAAAAAAAAAAQAAAAAAAAPA/AAAAAAAAAEAAAAAAAADwPwAAAAAAAPA/\",\"srid\":null},\"linestring_c\":{\"wkb\":\"AQIAAAADAAAAAAAAAAAACEAAAAAAAAAAAAAAAAAAAAhAAAAAAAAACEAAAAAAAAAIQAAAAAAAABRA\",\"srid\":null},\"polygon_c\":{\"wkb\":\"AQMAAAABAAAABQAAAAAAAAAAAPA/AAAAAAAA8D8AAAAAAAAAQAAAAAAAAPA/AAAAAAAAAEAAAAAAAAAAQAAAAAAAAPA/AAAAAAAAAEAAAAAAAADwPwAAAAAAAPA/\",\"srid\":null},\"multipoint_c\":{\"wkb\":\"AQQAAAACAAAAAQEAAAAAAAAAAADwPwAAAAAAAPA/AQEAAAAAAAAAAAAAQAAAAAAAAABA\",\"srid\":null},\"multiline_c\":{\"wkb\":\"AQUAAAACAAAAAQIAAAADAAAAAAAAAAAA8D8AAAAAAADwPwAAAAAAAABAAAAAAAAAAEAAAAAAAAAIQAAAAAAAAAhAAQIAAAACAAAAAAAAAAAAEEAAAAAAAAAQQAAAAAAAABRAAAAAAAAAFEA=\",\"srid\":null},\"multipolygon_c\":{\"wkb\":\"AQYAAAACAAAAAQMAAAABAAAABQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAkQAAAAAAAAAAAAAAAAAAAJEAAAAAAAAAkQAAAAAAAAAAAAAAAAAAAJEAAAAAAAAAAAAAAAAAAAAAAAQMAAAABAAAABQAAAAAAAAAAABRAAAAAAAAAFEAAAAAAAAAcQAAAAAAAABRAAAAAAAAAHEAAAAAAAAAcQAAAAAAAABRAAAAAAAAAHEAAAAAAAAAUQAAAAAAAABRA\",\"srid\":null},\"geometrycollection_c\":{\"wkb\":\"AQcAAAADAAAAAQEAAAAAAAAAAAAkQAAAAAAAACRAAQEAAAAAAAAAAAA+QAAAAAAAAD5AAQIAAAACAAAAAAAAAAAALkAAAAAAAAAuQAAAAAAAADRAAAAAAAAANEA=\",\"srid\":null}},\"source\":{\"version\":\"1.9.8.Final\",\"connector\":\"mysql\",\"name\":\"mysql_test\",\"ts_ms\":1720517925000,\"snapshot\":\"false\",\"db\":\"doris_test\",\"sequence\":null,\"table\":\"full_types\",\"server_id\":1,\"gtid\":null,\"file\":\"mysql-bin.000401\",\"pos\":17237,\"row\":0,\"thread\":5,\"query\":null},\"op\":\"c\",\"ts_ms\":1720518881444,\"transaction\":null}}";

        produceMsg2Kafka(topic, msg1);

        String tableSql =
                loadContent(
                        "src/test/resources/e2e/string_converter/full_types_debezium_ingestion.sql");
        createTable(tableSql);
        Thread.sleep(2000);
        kafkaContainerService.registerKafkaConnector(connectorName, jsonMsgConnectorContent);

        String table = dorisOptions.getTopicMapTable(topic);
        List<String> expected =
                Arrays.asList(
                        "1,127,255,255,32767,65535,65535,8388607,16777215,16777215,2147483647,4294967295,4294967295,2147483647,9223372036854775807,-1,-1,Hello World,abc,123.102,123.102,123.103,123.104,404.4443,404.4444,404.4445,123.4567,123.4568,123.4569,346,34567892.1,false,true,true,2020-07-17,18:00:22,2020-07-17T18:00:22,2020-07-17T18:00:22,text,2021,red,a,b,{\"key1\":\"value1\"},{coordinates=[3,1], type=Point, srid=0},{coordinates=[[[1,1],[2,1],[2,2],[1,2],[1,1]]], type=Polygon, srid=0},{coordinates=[[3,0],[3,3],[3,5]], type=LineString, srid=0},{coordinates=[[[1,1],[2,1],[2,2],[1,2],[1,1]]], type=Polygon, srid=0},{coordinates=[[1,1],[2,2]], type=MultiPoint, srid=0},{coordinates=[[[1,1],[2,2],[3,3]],[[4,4],[5,5]]], type=MultiLineString, srid=0},{coordinates=[[[[0,0],[10,0],[10,10],[0,10],[0,0]]],[[[5,5],[7,5],[7,7],[5,7],[5,5]]]], type=MultiPolygon, srid=0},{geometries=[{\"type\":\"Point\",\"coordinates\":[10,10]},{\"type\":\"Point\",\"coordinates\":[30,30]},{\"type\":\"LineString\",\"coordinates\":[[15,15],[20,20]]}], type=GeometryCollection, srid=0}");
        Thread.sleep(10000);
        String query = String.format("select * from %s.%s order by id", database, table);
        checkResult(expected, query, 51);
    }

    @Test
    public void testTimeExampleTypes() throws Exception {
        initialize("src/test/resources/e2e/string_converter/time_types.json");
        String topic = "time_example";
        String msg1 =
                "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"default\":0,\"field\":\"id\"},{\"type\":\"int64\",\"optional\":true,\"name\":\"io.debezium.time.MicroTimestamp\",\"version\":1,\"field\":\"timestamp_without_timezone\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.time.ZonedTimestamp\",\"version\":1,\"field\":\"timestamp_with_timezone\"},{\"type\":\"int32\",\"optional\":true,\"name\":\"io.debezium.time.Date\",\"version\":1,\"field\":\"date_only\"},{\"type\":\"int64\",\"optional\":true,\"name\":\"io.debezium.time.MicroTime\",\"version\":1,\"field\":\"time_without_timezone\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.time.ZonedTime\",\"version\":1,\"field\":\"time_with_timezone\"},{\"type\":\"int64\",\"optional\":true,\"name\":\"io.debezium.time.MicroDuration\",\"version\":1,\"field\":\"interval_period\"}],\"optional\":true,\"name\":\"pg.public.time_example.Value\",\"field\":\"before\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"default\":0,\"field\":\"id\"},{\"type\":\"int64\",\"optional\":true,\"name\":\"io.debezium.time.MicroTimestamp\",\"version\":1,\"field\":\"timestamp_without_timezone\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.time.ZonedTimestamp\",\"version\":1,\"field\":\"timestamp_with_timezone\"},{\"type\":\"int32\",\"optional\":true,\"name\":\"io.debezium.time.Date\",\"version\":1,\"field\":\"date_only\"},{\"type\":\"int64\",\"optional\":true,\"name\":\"io.debezium.time.MicroTime\",\"version\":1,\"field\":\"time_without_timezone\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.time.ZonedTime\",\"version\":1,\"field\":\"time_with_timezone\"},{\"type\":\"int64\",\"optional\":true,\"name\":\"io.debezium.time.MicroDuration\",\"version\":1,\"field\":\"interval_period\"}],\"optional\":true,\"name\":\"pg.public.time_example.Value\",\"field\":\"after\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"version\"},{\"type\":\"string\",\"optional\":false,\"field\":\"connector\"},{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"ts_ms\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.data.Enum\",\"version\":1,\"parameters\":{\"allowed\":\"true,last,false,incremental\"},\"default\":\"false\",\"field\":\"snapshot\"},{\"type\":\"string\",\"optional\":false,\"field\":\"db\"},{\"type\":\"string\",\"optional\":true,\"field\":\"sequence\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_us\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_ns\"},{\"type\":\"string\",\"optional\":false,\"field\":\"schema\"},{\"type\":\"string\",\"optional\":false,\"field\":\"table\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"txId\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"lsn\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"xmin\"}],\"optional\":false,\"name\":\"io.debezium.connector.postgresql.Source\",\"field\":\"source\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"id\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"total_order\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"data_collection_order\"}],\"optional\":true,\"name\":\"event.block\",\"version\":1,\"field\":\"transaction\"},{\"type\":\"string\",\"optional\":false,\"field\":\"op\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_ms\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_us\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_ns\"}],\"optional\":false,\"name\":\"pg.public.time_example.Envelope\",\"version\":2},\"payload\":{\"before\":null,\"after\":{\"id\":1,\"timestamp_without_timezone\":1725885296000000,\"timestamp_with_timezone\":\"2024-09-09T04:34:56.000000Z\",\"date_only\":19975,\"time_without_timezone\":45296000000,\"time_with_timezone\":\"04:34:56Z\",\"interval_period\":37091106000000},\"source\":{\"version\":\"2.7.2.Final\",\"connector\":\"postgresql\",\"name\":\"pg\",\"ts_ms\":1725875246492,\"snapshot\":\"false\",\"db\":\"wdl\",\"sequence\":\"[\\\"24499552\\\",\\\"24499656\\\"]\",\"ts_us\":1725875246492139,\"ts_ns\":1725875246492139000,\"schema\":\"public\",\"table\":\"time_example\",\"txId\":772,\"lsn\":24499656,\"xmin\":null},\"transaction\":null,\"op\":\"c\",\"ts_ms\":1725875246751,\"ts_us\":1725875246751420,\"ts_ns\":1725875246751420000}}";
        produceMsg2Kafka(topic, msg1);

        String tableSql = loadContent("src/test/resources/e2e/string_converter/time_types.sql");
        createTable(tableSql);
        Thread.sleep(2000);
        kafkaContainerService.registerKafkaConnector(connectorName, jsonMsgConnectorContent);

        String table = dorisOptions.getTopicMapTable(topic);
        List<String> expected =
                Collections.singletonList(
                        "1,2024-09-09T12:34:56,2024-09-09T12:34:56,2024-09-09,12:34:56,12:34:56,37091106000000");
        Thread.sleep(10000);
        String query = String.format("select * from %s.%s order by id", database, table);
        checkResult(expected, query, 7);
    }

    @Test
    public void testTableFieldConfig() throws Exception {
        initialize("src/test/resources/e2e/string_converter/table_field_config.json");
        String topic = "table_field_config_test";
        String msg1 =
                "{\"id\":1,\"col1\":\"col1\",\"col2\":\"col2\",\"table_name\":\"field_config_tab1\"}";
        String msg2 =
                "{\"id\":1,\"col1\":\"col1\",\"col2\":\"col2\",\"table_name\":\"field_config_tab2\"}";

        produceMsg2Kafka(topic, msg1);
        produceMsg2Kafka(topic, msg2);

        String tableSql1 =
                loadContent("src/test/resources/e2e/string_converter/table_field_config1.sql");
        createTable(tableSql1);
        String tableSql2 =
                loadContent("src/test/resources/e2e/string_converter/table_field_config2.sql");
        createTable(tableSql2);

        Thread.sleep(2000);
        kafkaContainerService.registerKafkaConnector(connectorName, jsonMsgConnectorContent);

        List<String> expected = Collections.singletonList("1,col1,col2");
        Thread.sleep(10000);
        String query1 =
                String.format(
                        "select id,col1,col2 from %s.%s order by id",
                        database, "field_config_tab1");
        checkResult(expected, query1, 3);
        String query2 =
                String.format(
                        "select id,col1,col2 from %s.%s order by id",
                        database, "field_config_tab2");
        checkResult(expected, query2, 3);
    }

    @Test
    public void testRenameTransform() throws Exception {
        initialize("src/test/resources/e2e/transforms/rename_transforms.json");
        String topic = "kf_rename_transform_msg";
        String msg1 = "{\"id\":1,\"old_col1\":\"col1\",\"col2\":\"col2\"}";
        String msg2 = "{\"id\":2,\"old_col1\":\"col1_1\",\"col2\":\"col2\"}";
        produceMsg2Kafka(topic, msg1);
        produceMsg2Kafka(topic, msg2);

        String tableSql1 = loadContent("src/test/resources/e2e/transforms/rename_transforms.sql");
        createTable(tableSql1);

        Thread.sleep(2000);
        kafkaContainerService.registerKafkaConnector(connectorName, jsonMsgConnectorContent);

        List<String> expectedResult = Arrays.asList("1,col1,col2", "2,col1_1,col2");
        Thread.sleep(10000);
        String query1 =
                String.format(
                        "select id,col1,col2 from %s.%s order by id",
                        database, "rename_transform_msg");
        checkResult(expectedResult, query1, 3);
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
        LOG.info("expected result: {}", Arrays.toString(expected.toArray()));
        LOG.info("actual result: {}", Arrays.toString(actual.toArray()));
        Assert.assertArrayEquals(expected.toArray(), actual.toArray());
    }

    @AfterClass
    public static void closeInstance() {
        kafkaContainerService.deleteKafkaConnector(connectorName);
    }
}
