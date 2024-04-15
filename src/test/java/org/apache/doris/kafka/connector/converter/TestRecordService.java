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

package org.apache.doris.kafka.connector.converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.doris.kafka.connector.cfg.DorisOptions;
import org.apache.doris.kafka.connector.writer.TestRecordBuffer;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestRecordService {

    private RecordService recordService;
    private Properties props = new Properties();
    private JsonConverter jsonConverter = new JsonConverter();

    @Before
    public void init() throws IOException {
        InputStream stream =
                this.getClass()
                        .getClassLoader()
                        .getResourceAsStream("doris-connector-sink.properties");
        props.load(stream);
        props.put("task_id", "1");
        props.put("converter.mode", "debezium_ingestion");
        recordService = new RecordService(new DorisOptions((Map) props));
        HashMap<String, String> config = new HashMap<>();
        jsonConverter.configure(config, false);
    }

    /**
     * The mysql table schema is as follows.
     *
     * <p>CREATE TABLE example_table ( id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50), age INT,
     * email VARCHAR(100), birth_date DATE, integer_column INT, float_column FLOAT, decimal_column
     * DECIMAL(10,2), datetime_column DATETIME, date_column DATE, time_column TIME, text_column
     * TEXT, varchar_column VARCHAR(255), binary_column BINARY(10), blob_column BLOB, is_active
     * TINYINT(1) );
     */
    @Test
    public void processMysqlDebeziumStructRecord() throws IOException {
        String topic = "normal.wdl_test.example_table";
        // no delete value
        String noDeleteValue =
                "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"age\"},{\"type\":\"string\",\"optional\":true,\"field\":\"email\"},{\"type\":\"int32\",\"optional\":true,\"name\":\"io.debezium.time.Date\",\"version\":1,\"field\":\"birth_date\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"integer_column\"},{\"type\":\"float\",\"optional\":true,\"field\":\"float_column\"},{\"type\":\"bytes\",\"optional\":true,\"name\":\"org.apache.kafka.connect.data.Decimal\",\"version\":1,\"parameters\":{\"scale\":\"2\",\"connect.decimal.precision\":\"10\"},\"field\":\"decimal_column\"},{\"type\":\"int64\",\"optional\":true,\"name\":\"io.debezium.time.Timestamp\",\"version\":1,\"field\":\"datetime_column\"},{\"type\":\"int32\",\"optional\":true,\"name\":\"io.debezium.time.Date\",\"version\":1,\"field\":\"date_column\"},{\"type\":\"int64\",\"optional\":true,\"name\":\"io.debezium.time.MicroTime\",\"version\":1,\"field\":\"time_column\"},{\"type\":\"string\",\"optional\":true,\"field\":\"text_column\"},{\"type\":\"string\",\"optional\":true,\"field\":\"varchar_column\"},{\"type\":\"bytes\",\"optional\":true,\"field\":\"binary_column\"},{\"type\":\"bytes\",\"optional\":true,\"field\":\"blob_column\"},{\"type\":\"int16\",\"optional\":true,\"field\":\"is_active\"}],\"optional\":true,\"name\":\"normal.wdl_test.example_table.Value\",\"field\":\"before\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"age\"},{\"type\":\"string\",\"optional\":true,\"field\":\"email\"},{\"type\":\"int32\",\"optional\":true,\"name\":\"io.debezium.time.Date\",\"version\":1,\"field\":\"birth_date\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"integer_column\"},{\"type\":\"float\",\"optional\":true,\"field\":\"float_column\"},{\"type\":\"bytes\",\"optional\":true,\"name\":\"org.apache.kafka.connect.data.Decimal\",\"version\":1,\"parameters\":{\"scale\":\"2\",\"connect.decimal.precision\":\"10\"},\"field\":\"decimal_column\"},{\"type\":\"int64\",\"optional\":true,\"name\":\"io.debezium.time.Timestamp\",\"version\":1,\"field\":\"datetime_column\"},{\"type\":\"int32\",\"optional\":true,\"name\":\"io.debezium.time.Date\",\"version\":1,\"field\":\"date_column\"},{\"type\":\"int64\",\"optional\":true,\"name\":\"io.debezium.time.MicroTime\",\"version\":1,\"field\":\"time_column\"},{\"type\":\"string\",\"optional\":true,\"field\":\"text_column\"},{\"type\":\"string\",\"optional\":true,\"field\":\"varchar_column\"},{\"type\":\"bytes\",\"optional\":true,\"field\":\"binary_column\"},{\"type\":\"bytes\",\"optional\":true,\"field\":\"blob_column\"},{\"type\":\"int16\",\"optional\":true,\"field\":\"is_active\"}],\"optional\":true,\"name\":\"normal.wdl_test.example_table.Value\",\"field\":\"after\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"version\"},{\"type\":\"string\",\"optional\":false,\"field\":\"connector\"},{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"ts_ms\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.data.Enum\",\"version\":1,\"parameters\":{\"allowed\":\"true,last,false,incremental\"},\"default\":\"false\",\"field\":\"snapshot\"},{\"type\":\"string\",\"optional\":false,\"field\":\"db\"},{\"type\":\"string\",\"optional\":true,\"field\":\"sequence\"},{\"type\":\"string\",\"optional\":true,\"field\":\"table\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"server_id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"gtid\"},{\"type\":\"string\",\"optional\":false,\"field\":\"file\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"pos\"},{\"type\":\"int32\",\"optional\":false,\"field\":\"row\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"thread\"},{\"type\":\"string\",\"optional\":true,\"field\":\"query\"}],\"optional\":false,\"name\":\"io.debezium.connector.mysql.Source\",\"field\":\"source\"},{\"type\":\"string\",\"optional\":false,\"field\":\"op\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_ms\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"id\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"total_order\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"data_collection_order\"}],\"optional\":true,\"name\":\"event.block\",\"version\":1,\"field\":\"transaction\"}],\"optional\":false,\"name\":\"normal.wdl_test.example_table.Envelope\",\"version\":1},\"payload\":{\"before\":null,\"after\":{\"id\":8,\"name\":\"Jfohn Doe\",\"age\":430,\"email\":\"john@example.com\",\"birth_date\":8905,\"integer_column\":12323,\"float_column\":45.67,\"decimal_column\":\"MDk=\",\"datetime_column\":1712917800000,\"date_column\":19825,\"time_column\":37800000000,\"text_column\":\"Lorem ipsum dolor sit amet, consectetur adipiscing elit.\",\"varchar_column\":null,\"binary_column\":\"EjRWeJCrze8AAA==\",\"blob_column\":null,\"is_active\":2},\"source\":{\"version\":\"2.5.4.Final\",\"connector\":\"mysql\",\"name\":\"normal\",\"ts_ms\":1712915126000,\"snapshot\":\"false\",\"db\":\"wdl_test\",\"sequence\":null,\"table\":\"example_table\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000063\",\"pos\":13454,\"row\":0,\"thread\":20,\"query\":null},\"op\":\"c\",\"ts_ms\":1712915126481,\"transaction\":null}}";
        String expectedNoDeleteValue =
                "{\"id\":8,\"name\":\"Jfohn Doe\",\"age\":430,\"email\":\"john@example.com\",\"birth_date\":\"1994-05-20\",\"integer_column\":12323,\"float_column\":45.67,\"decimal_column\":123.45,\"datetime_column\":\"2024-04-12T10:30\",\"date_column\":\"2024-04-12\",\"time_column\":\"2024-04-15T10:30\",\"text_column\":\"Lorem ipsum dolor sit amet, consectetur adipiscing elit.\",\"varchar_column\":null,\"binary_column\":\"1234567890ABCDEF0000\",\"blob_column\":null,\"is_active\":2,\"__DORIS_DELETE_SIGN__\":\"0\"}";
        buildProcessStructRecord(topic, noDeleteValue, expectedNoDeleteValue);

        // delete value
        String deleteValue =
                "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"age\"},{\"type\":\"string\",\"optional\":true,\"field\":\"email\"},{\"type\":\"int32\",\"optional\":true,\"name\":\"io.debezium.time.Date\",\"version\":1,\"field\":\"birth_date\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"integer_column\"},{\"type\":\"float\",\"optional\":true,\"field\":\"float_column\"},{\"type\":\"bytes\",\"optional\":true,\"name\":\"org.apache.kafka.connect.data.Decimal\",\"version\":1,\"parameters\":{\"scale\":\"2\",\"connect.decimal.precision\":\"10\"},\"field\":\"decimal_column\"},{\"type\":\"int64\",\"optional\":true,\"name\":\"io.debezium.time.Timestamp\",\"version\":1,\"field\":\"datetime_column\"},{\"type\":\"int32\",\"optional\":true,\"name\":\"io.debezium.time.Date\",\"version\":1,\"field\":\"date_column\"},{\"type\":\"int64\",\"optional\":true,\"name\":\"io.debezium.time.MicroTime\",\"version\":1,\"field\":\"time_column\"},{\"type\":\"string\",\"optional\":true,\"field\":\"text_column\"},{\"type\":\"string\",\"optional\":true,\"field\":\"varchar_column\"},{\"type\":\"bytes\",\"optional\":true,\"field\":\"binary_column\"},{\"type\":\"bytes\",\"optional\":true,\"field\":\"blob_column\"},{\"type\":\"int16\",\"optional\":true,\"field\":\"is_active\"}],\"optional\":true,\"name\":\"normal.wdl_test.example_table.Value\",\"field\":\"before\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"age\"},{\"type\":\"string\",\"optional\":true,\"field\":\"email\"},{\"type\":\"int32\",\"optional\":true,\"name\":\"io.debezium.time.Date\",\"version\":1,\"field\":\"birth_date\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"integer_column\"},{\"type\":\"float\",\"optional\":true,\"field\":\"float_column\"},{\"type\":\"bytes\",\"optional\":true,\"name\":\"org.apache.kafka.connect.data.Decimal\",\"version\":1,\"parameters\":{\"scale\":\"2\",\"connect.decimal.precision\":\"10\"},\"field\":\"decimal_column\"},{\"type\":\"int64\",\"optional\":true,\"name\":\"io.debezium.time.Timestamp\",\"version\":1,\"field\":\"datetime_column\"},{\"type\":\"int32\",\"optional\":true,\"name\":\"io.debezium.time.Date\",\"version\":1,\"field\":\"date_column\"},{\"type\":\"int64\",\"optional\":true,\"name\":\"io.debezium.time.MicroTime\",\"version\":1,\"field\":\"time_column\"},{\"type\":\"string\",\"optional\":true,\"field\":\"text_column\"},{\"type\":\"string\",\"optional\":true,\"field\":\"varchar_column\"},{\"type\":\"bytes\",\"optional\":true,\"field\":\"binary_column\"},{\"type\":\"bytes\",\"optional\":true,\"field\":\"blob_column\"},{\"type\":\"int16\",\"optional\":true,\"field\":\"is_active\"}],\"optional\":true,\"name\":\"normal.wdl_test.example_table.Value\",\"field\":\"after\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"version\"},{\"type\":\"string\",\"optional\":false,\"field\":\"connector\"},{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"ts_ms\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.data.Enum\",\"version\":1,\"parameters\":{\"allowed\":\"true,last,false,incremental\"},\"default\":\"false\",\"field\":\"snapshot\"},{\"type\":\"string\",\"optional\":false,\"field\":\"db\"},{\"type\":\"string\",\"optional\":true,\"field\":\"sequence\"},{\"type\":\"string\",\"optional\":true,\"field\":\"table\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"server_id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"gtid\"},{\"type\":\"string\",\"optional\":false,\"field\":\"file\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"pos\"},{\"type\":\"int32\",\"optional\":false,\"field\":\"row\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"thread\"},{\"type\":\"string\",\"optional\":true,\"field\":\"query\"}],\"optional\":false,\"name\":\"io.debezium.connector.mysql.Source\",\"field\":\"source\"},{\"type\":\"string\",\"optional\":false,\"field\":\"op\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_ms\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"id\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"total_order\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"data_collection_order\"}],\"optional\":true,\"name\":\"event.block\",\"version\":1,\"field\":\"transaction\"}],\"optional\":false,\"name\":\"normal.wdl_test.example_table.Envelope\",\"version\":1},\"payload\":{\"before\":{\"id\":8,\"name\":\"Jfohn Doe\",\"age\":430,\"email\":\"john@example.com\",\"birth_date\":8905,\"integer_column\":12323,\"float_column\":45.67,\"decimal_column\":\"MDk=\",\"datetime_column\":1712917800000,\"date_column\":19825,\"time_column\":37800000000,\"text_column\":\"Lorem ipsum dolor sit amet, consectetur adipiscing elit.\",\"varchar_column\":null,\"binary_column\":\"EjRWeJCrze8AAA==\",\"blob_column\":null,\"is_active\":2},\"after\":null,\"source\":{\"version\":\"2.5.4.Final\",\"connector\":\"mysql\",\"name\":\"normal\",\"ts_ms\":1712915202000,\"snapshot\":\"false\",\"db\":\"wdl_test\",\"sequence\":null,\"table\":\"example_table\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000063\",\"pos\":13915,\"row\":0,\"thread\":20,\"query\":null},\"op\":\"d\",\"ts_ms\":1712915202020,\"transaction\":null}}";
        String expectedDeleteValue =
                "{\"id\":8,\"name\":\"Jfohn Doe\",\"age\":430,\"email\":\"john@example.com\",\"birth_date\":\"1994-05-20\",\"integer_column\":12323,\"float_column\":45.67,\"decimal_column\":123.45,\"datetime_column\":\"2024-04-12T10:30\",\"date_column\":\"2024-04-12\",\"time_column\":\"2024-04-15T10:30\",\"text_column\":\"Lorem ipsum dolor sit amet, consectetur adipiscing elit.\",\"varchar_column\":null,\"binary_column\":\"1234567890ABCDEF0000\",\"blob_column\":null,\"is_active\":2,\"__DORIS_DELETE_SIGN__\":\"1\"}";
        buildProcessStructRecord(topic, deleteValue, expectedDeleteValue);
    }

    private void buildProcessStructRecord(String topic, String sourceValue, String target)
            throws IOException {
        SchemaAndValue noDeleteSchemaValue =
                jsonConverter.toConnectData(topic, sourceValue.getBytes(StandardCharsets.UTF_8));
        SinkRecord noDeleteSinkRecord =
                TestRecordBuffer.newSinkRecord(
                        noDeleteSchemaValue.value(), 8, noDeleteSchemaValue.schema());
        String processResult = recordService.processStructRecord(noDeleteSinkRecord);
        Assert.assertEquals(target, processResult);
    }

    @Test
    public void processStructRecord() throws IOException {
        props.remove("converter.mode");
        recordService = new RecordService(new DorisOptions((Map) props));
        String topic = "normal.wdl_test.test_sink_normal";

        // no delete value
        String noDeleteValue =
                "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"}],\"optional\":true,\"name\":\"normal.wdl_test.test_sink_normal.Value\",\"field\":\"before\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"}],\"optional\":true,\"name\":\"normal.wdl_test.test_sink_normal.Value\",\"field\":\"after\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"version\"},{\"type\":\"string\",\"optional\":false,\"field\":\"connector\"},{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"ts_ms\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.data.Enum\",\"version\":1,\"parameters\":{\"allowed\":\"true,last,false,incremental\"},\"default\":\"false\",\"field\":\"snapshot\"},{\"type\":\"string\",\"optional\":false,\"field\":\"db\"},{\"type\":\"string\",\"optional\":true,\"field\":\"sequence\"},{\"type\":\"string\",\"optional\":true,\"field\":\"table\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"server_id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"gtid\"},{\"type\":\"string\",\"optional\":false,\"field\":\"file\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"pos\"},{\"type\":\"int32\",\"optional\":false,\"field\":\"row\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"thread\"},{\"type\":\"string\",\"optional\":true,\"field\":\"query\"}],\"optional\":false,\"name\":\"io.debezium.connector.mysql.Source\",\"field\":\"source\"},{\"type\":\"string\",\"optional\":false,\"field\":\"op\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_ms\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"id\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"total_order\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"data_collection_order\"}],\"optional\":true,\"name\":\"event.block\",\"version\":1,\"field\":\"transaction\"}],\"optional\":false,\"name\":\"normal.wdl_test.test_sink_normal.Envelope\",\"version\":1},\"payload\":{\"before\":null,\"after\":{\"id\":19,\"name\":\"fff\"},\"source\":{\"version\":\"2.5.4.Final\",\"connector\":\"mysql\",\"name\":\"normal\",\"ts_ms\":1712543697000,\"snapshot\":\"false\",\"db\":\"wdl_test\",\"sequence\":null,\"table\":\"test_sink_normal\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000061\",\"pos\":5320,\"row\":0,\"thread\":260,\"query\":null},\"op\":\"c\",\"ts_ms\":1712543697062,\"transaction\":null}}";
        String expectedNoDeleteValue =
                "{\"before\":null,\"after\":{\"id\":19,\"name\":\"fff\"},\"source\":{\"version\":\"2.5.4.Final\",\"connector\":\"mysql\",\"name\":\"normal\",\"ts_ms\":1712543697000,\"snapshot\":\"false\",\"db\":\"wdl_test\",\"sequence\":null,\"table\":\"test_sink_normal\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000061\",\"pos\":5320,\"row\":0,\"thread\":260,\"query\":null},\"op\":\"c\",\"ts_ms\":1712543697062,\"transaction\":null}";
        buildProcessStructRecord(topic, noDeleteValue, expectedNoDeleteValue);
    }

    @Test
    public void processStructRecordWithDebeziumSchema() throws IOException {
        String topic = "normal.wdl_test.test_sink_normal";

        // no delete value
        String noDeleteValue =
                "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"}],\"optional\":true,\"name\":\"normal.wdl_test.test_sink_normal.Value\",\"field\":\"before\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"}],\"optional\":true,\"name\":\"normal.wdl_test.test_sink_normal.Value\",\"field\":\"after\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"version\"},{\"type\":\"string\",\"optional\":false,\"field\":\"connector\"},{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"ts_ms\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.data.Enum\",\"version\":1,\"parameters\":{\"allowed\":\"true,last,false,incremental\"},\"default\":\"false\",\"field\":\"snapshot\"},{\"type\":\"string\",\"optional\":false,\"field\":\"db\"},{\"type\":\"string\",\"optional\":true,\"field\":\"sequence\"},{\"type\":\"string\",\"optional\":true,\"field\":\"table\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"server_id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"gtid\"},{\"type\":\"string\",\"optional\":false,\"field\":\"file\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"pos\"},{\"type\":\"int32\",\"optional\":false,\"field\":\"row\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"thread\"},{\"type\":\"string\",\"optional\":true,\"field\":\"query\"}],\"optional\":false,\"name\":\"io.debezium.connector.mysql.Source\",\"field\":\"source\"},{\"type\":\"string\",\"optional\":false,\"field\":\"op\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_ms\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"id\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"total_order\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"data_collection_order\"}],\"optional\":true,\"name\":\"event.block\",\"version\":1,\"field\":\"transaction\"}],\"optional\":false,\"name\":\"normal.wdl_test.test_sink_normal.Envelope\",\"version\":1},\"payload\":{\"before\":null,\"after\":{\"id\":19,\"name\":\"fff\"},\"source\":{\"version\":\"2.5.4.Final\",\"connector\":\"mysql\",\"name\":\"normal\",\"ts_ms\":1712543697000,\"snapshot\":\"false\",\"db\":\"wdl_test\",\"sequence\":null,\"table\":\"test_sink_normal\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000061\",\"pos\":5320,\"row\":0,\"thread\":260,\"query\":null},\"op\":\"c\",\"ts_ms\":1712543697062,\"transaction\":null}}";
        String expectedNoDeleteValue =
                "{\"id\":19,\"name\":\"fff\",\"__DORIS_DELETE_SIGN__\":\"0\"}";
        buildProcessStructRecord(topic, noDeleteValue, expectedNoDeleteValue);

        // delete value
        String deleteValue =
                "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"}],\"optional\":true,\"name\":\"normal.wdl_test.test_sink_normal.Value\",\"field\":\"before\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"}],\"optional\":true,\"name\":\"normal.wdl_test.test_sink_normal.Value\",\"field\":\"after\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"version\"},{\"type\":\"string\",\"optional\":false,\"field\":\"connector\"},{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"ts_ms\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.data.Enum\",\"version\":1,\"parameters\":{\"allowed\":\"true,last,false,incremental\"},\"default\":\"false\",\"field\":\"snapshot\"},{\"type\":\"string\",\"optional\":false,\"field\":\"db\"},{\"type\":\"string\",\"optional\":true,\"field\":\"sequence\"},{\"type\":\"string\",\"optional\":true,\"field\":\"table\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"server_id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"gtid\"},{\"type\":\"string\",\"optional\":false,\"field\":\"file\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"pos\"},{\"type\":\"int32\",\"optional\":false,\"field\":\"row\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"thread\"},{\"type\":\"string\",\"optional\":true,\"field\":\"query\"}],\"optional\":false,\"name\":\"io.debezium.connector.mysql.Source\",\"field\":\"source\"},{\"type\":\"string\",\"optional\":false,\"field\":\"op\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_ms\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"id\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"total_order\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"data_collection_order\"}],\"optional\":true,\"name\":\"event.block\",\"version\":1,\"field\":\"transaction\"}],\"optional\":false,\"name\":\"normal.wdl_test.test_sink_normal.Envelope\",\"version\":1},\"payload\":{\"before\":{\"id\":24,\"name\":\"bb\"},\"after\":null,\"source\":{\"version\":\"2.5.4.Final\",\"connector\":\"mysql\",\"name\":\"normal\",\"ts_ms\":1712545844000,\"snapshot\":\"false\",\"db\":\"wdl_test\",\"sequence\":null,\"table\":\"test_sink_normal\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000061\",\"pos\":5627,\"row\":0,\"thread\":260,\"query\":null},\"op\":\"d\",\"ts_ms\":1712545844948,\"transaction\":null}}";
        String expectedDeleteValue = "{\"id\":24,\"name\":\"bb\",\"__DORIS_DELETE_SIGN__\":\"1\"}";
        buildProcessStructRecord(topic, deleteValue, expectedDeleteValue);
    }

    @Test
    public void processListRecord() throws JsonProcessingException {
        List<Map<String, String>> list = new ArrayList<>();
        Map<String, String> mapValue = new HashMap<>();
        mapValue.put("name", "doris");
        mapValue.put("key", "1");

        Map<String, String> mapValue2 = new HashMap<>();
        mapValue2.put("name", "doris");
        mapValue2.put("key", "2");
        list.add(mapValue);
        list.add(mapValue2);

        SinkRecord record = TestRecordBuffer.newSinkRecord(list, 1);
        String s = recordService.processListRecord(record);
        Assert.assertEquals(
                "{\"name\":\"doris\",\"key\":\"1\"}\n{\"name\":\"doris\",\"key\":\"2\"}", s);
    }

    @Test
    public void processMapRecord() throws JsonProcessingException {
        Map<String, String> mapValue = new HashMap<>();
        mapValue.put("name", "doris");
        mapValue.put("key", "1");
        SinkRecord record = TestRecordBuffer.newSinkRecord(mapValue, 1);
        String s = recordService.processMapRecord(record);
        Assert.assertEquals("{\"name\":\"doris\",\"key\":\"1\"}", s);

        ObjectNode objectNode = new ObjectMapper().createObjectNode();
        objectNode.put("name", "doris");
        objectNode.put("key", "1");
        Assert.assertEquals(
                "{\"name\":\"doris\",\"key\":\"1\"}",
                new ObjectMapper().writeValueAsString(objectNode));
    }

    @Test
    public void processStringRecord() {
        SinkRecord record = TestRecordBuffer.newSinkRecord("doris", 1);
        String s = recordService.processStringRecord(record);
        Assert.assertEquals("doris", s);
    }
}
