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

package org.apache.doris.kafka.connector.service;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.doris.kafka.connector.cfg.DorisSinkConnectorConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestDorisSinkService {

    private DorisDefaultSinkService dorisDefaultSinkService;
    private JsonConverter jsonConverter = new JsonConverter();

    @Before
    public void init() throws IOException {
        InputStream stream =
                this.getClass()
                        .getClassLoader()
                        .getResourceAsStream("doris-connector-sink.properties");
        Properties props = new Properties();
        props.load(stream);
        DorisSinkConnectorConfig.setDefaultValues((Map) props);
        props.put("task_id", "1");
        props.put("name", "sink-connector-test");
        props.put("record.tablename.field", "table_name");
        dorisDefaultSinkService = new DorisDefaultSinkService((Map) props);
        jsonConverter.configure(new HashMap<>(), false);
    }

    @Test
    public void getSinkDorisTableName() {
        SinkRecord record1 =
                new SinkRecord(
                        "topic_test",
                        0,
                        Schema.OPTIONAL_STRING_SCHEMA,
                        "key",
                        Schema.OPTIONAL_STRING_SCHEMA,
                        "val",
                        1);
        Assert.assertEquals(
                "test_kafka_tbl", dorisDefaultSinkService.getSinkDorisTableName(record1));

        Map<String, String> valueMap = new HashMap<>();
        valueMap.put("col1", "val");
        valueMap.put("table_name", "appoint_table");
        SinkRecord record2 =
                new SinkRecord(
                        "topic_test",
                        0,
                        Schema.OPTIONAL_STRING_SCHEMA,
                        "key",
                        SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA)
                                .optional()
                                .build(),
                        valueMap,
                        1);
        Assert.assertEquals(
                "appoint_table", dorisDefaultSinkService.getSinkDorisTableName(record2));

        String recordValue3 = "{\"id\":1,\"name\":\"bob\",\"age\":12}";
        SinkRecord record3 =
                new SinkRecord(
                        "topic_test",
                        0,
                        Schema.OPTIONAL_STRING_SCHEMA,
                        "key",
                        Schema.OPTIONAL_STRING_SCHEMA,
                        recordValue3,
                        3);
        Assert.assertEquals(
                "test_kafka_tbl", dorisDefaultSinkService.getSinkDorisTableName(record3));

        String recordValue4 =
                "{\"id\":12,\"name\":\"jack\",\"age\":13,\"table_name\":\"appoint_table2\"}";
        SinkRecord record4 =
                new SinkRecord(
                        "topic_test",
                        0,
                        Schema.OPTIONAL_STRING_SCHEMA,
                        "key",
                        Schema.OPTIONAL_STRING_SCHEMA,
                        recordValue4,
                        3);
        Assert.assertEquals(
                "appoint_table2", dorisDefaultSinkService.getSinkDorisTableName(record4));

        String structMsg =
                "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"}],\"optional\":true,\"name\":\"normal.test.test_sink_normal.Value\",\"field\":\"before\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"}],\"optional\":true,\"name\":\"normal.test.test_sink_normal.Value\",\"field\":\"after\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"version\"},{\"type\":\"string\",\"optional\":false,\"field\":\"connector\"},{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"ts_ms\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.data.Enum\",\"version\":1,\"parameters\":{\"allowed\":\"true,last,false,incremental\"},\"default\":\"false\",\"field\":\"snapshot\"},{\"type\":\"string\",\"optional\":false,\"field\":\"db\"},{\"type\":\"string\",\"optional\":true,\"field\":\"sequence\"},{\"type\":\"string\",\"optional\":true,\"field\":\"table\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"server_id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"gtid\"},{\"type\":\"string\",\"optional\":false,\"field\":\"file\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"pos\"},{\"type\":\"int32\",\"optional\":false,\"field\":\"row\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"thread\"},{\"type\":\"string\",\"optional\":true,\"field\":\"query\"}],\"optional\":false,\"name\":\"io.debezium.connector.mysql.Source\",\"field\":\"source\"},{\"type\":\"string\",\"optional\":false,\"field\":\"op\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_ms\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"id\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"total_order\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"data_collection_order\"}],\"optional\":true,\"name\":\"event.block\",\"version\":1,\"field\":\"transaction\"}],\"optional\":false,\"name\":\"normal.test.test_sink_normal.Envelope\",\"version\":1},\"payload\":{\"before\":null,\"after\":{\"id\":19,\"name\":\"fff\"},\"source\":{\"version\":\"2.5.4.Final\",\"connector\":\"mysql\",\"name\":\"normal\",\"ts_ms\":1712543697000,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"test_sink_normal\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000061\",\"pos\":5320,\"row\":0,\"thread\":260,\"query\":null},\"op\":\"c\",\"ts_ms\":1712543697062,\"transaction\":null}}";
        SchemaAndValue schemaAndValue =
                jsonConverter.toConnectData(
                        "topic_test", structMsg.getBytes(StandardCharsets.UTF_8));
        SinkRecord record5 =
                new SinkRecord(
                        "topic_test",
                        0,
                        Schema.OPTIONAL_STRING_SCHEMA,
                        "key",
                        Schema.OPTIONAL_STRING_SCHEMA,
                        new Struct(schemaAndValue.schema()),
                        3);
        Assert.assertEquals(
                "test_kafka_tbl", dorisDefaultSinkService.getSinkDorisTableName(record5));
    }
}
