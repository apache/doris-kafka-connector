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

package org.apache.doris.kafka.connector.writer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.doris.kafka.connector.cfg.DorisOptions;
import org.apache.doris.kafka.connector.connection.JdbcConnectionProvider;
import org.apache.doris.kafka.connector.exception.DorisException;
import org.apache.doris.kafka.connector.metrics.DorisConnectMonitor;
import org.apache.doris.kafka.connector.model.doris.Schema;
import org.apache.doris.kafka.connector.service.DorisSystemService;
import org.apache.doris.kafka.connector.service.RestService;
import org.apache.doris.kafka.connector.writer.schema.DebeziumSchemaChange;
import org.apache.doris.kafka.connector.writer.schema.SchemaChangeManager;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class TestDebeziumSchemaChange {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final JsonConverter jsonConverter = new JsonConverter();
    private final HashSet<String> sinkTableSet = new HashSet<>();
    private DebeziumSchemaChange debeziumSchemaChange;
    private DorisOptions dorisOptions;
    private String topic;
    private MockedStatic<RestService> mockRestService;

    @Before
    public void init() throws IOException {
        InputStream stream =
                this.getClass()
                        .getClassLoader()
                        .getResourceAsStream("doris-connector-sink.properties");
        Properties props = new Properties();
        props.load(stream);
        props.put("task_id", "1");
        props.put("name", "sink-connector-test");
        topic = "normal";
        dorisOptions = new DorisOptions((Map) props);
        DorisConnectMonitor dorisConnectMonitor = mock(DorisConnectMonitor.class);
        DorisSystemService mockDorisSystemService = mock(DorisSystemService.class);
        jsonConverter.configure(new HashMap<>(), false);
        mockRestService = mockStatic(RestService.class);
        SchemaChangeManager mockSchemaChangeManager = Mockito.mock(SchemaChangeManager.class);
        Mockito.when(
                        mockSchemaChangeManager.checkSchemaChange(
                                Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(true);
        debeziumSchemaChange =
                new DebeziumSchemaChange(
                        topic,
                        0,
                        dorisOptions,
                        new JdbcConnectionProvider(dorisOptions),
                        dorisConnectMonitor);
        when(mockDorisSystemService.tableExists(anyString(), anyString())).thenReturn(true);

        sinkTableSet.add("normal_time");
        debeziumSchemaChange.setSchemaChangeManager(mockSchemaChangeManager);
        debeziumSchemaChange.setSinkTableSet(sinkTableSet);
        debeziumSchemaChange.setDorisSystemService(mockDorisSystemService);
    }

    @Test
    public void testAlterSchemaChange() {
        String alterTopicMsg =
                "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"version\"},{\"type\":\"string\",\"optional\":false,\"field\":\"connector\"},{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"ts_ms\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.data.Enum\",\"version\":1,\"parameters\":{\"allowed\":\"true,last,false,incremental\"},\"default\":\"false\",\"field\":\"snapshot\"},{\"type\":\"string\",\"optional\":false,\"field\":\"db\"},{\"type\":\"string\",\"optional\":true,\"field\":\"sequence\"},{\"type\":\"string\",\"optional\":true,\"field\":\"table\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"server_id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"gtid\"},{\"type\":\"string\",\"optional\":false,\"field\":\"file\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"pos\"},{\"type\":\"int32\",\"optional\":false,\"field\":\"row\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"thread\"},{\"type\":\"string\",\"optional\":true,\"field\":\"query\"}],\"optional\":false,\"name\":\"io.debezium.connector.mysql.Source\",\"field\":\"source\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"ts_ms\"},{\"type\":\"string\",\"optional\":true,\"field\":\"databaseName\"},{\"type\":\"string\",\"optional\":true,\"field\":\"schemaName\"},{\"type\":\"string\",\"optional\":true,\"field\":\"ddl\"},{\"type\":\"array\",\"items\":{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"type\"},{\"type\":\"string\",\"optional\":false,\"field\":\"id\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":true,\"field\":\"defaultCharsetName\"},{\"type\":\"array\",\"items\":{\"type\":\"string\",\"optional\":false},\"optional\":true,\"field\":\"primaryKeyColumnNames\"},{\"type\":\"array\",\"items\":{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int32\",\"optional\":false,\"field\":\"jdbcType\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"nativeType\"},{\"type\":\"string\",\"optional\":false,\"field\":\"typeName\"},{\"type\":\"string\",\"optional\":true,\"field\":\"typeExpression\"},{\"type\":\"string\",\"optional\":true,\"field\":\"charsetName\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"length\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"scale\"},{\"type\":\"int32\",\"optional\":false,\"field\":\"position\"},{\"type\":\"boolean\",\"optional\":true,\"field\":\"optional\"},{\"type\":\"boolean\",\"optional\":true,\"field\":\"autoIncremented\"},{\"type\":\"boolean\",\"optional\":true,\"field\":\"generated\"},{\"type\":\"string\",\"optional\":true,\"field\":\"comment\"},{\"type\":\"string\",\"optional\":true,\"field\":\"defaultValueExpression\"},{\"type\":\"array\",\"items\":{\"type\":\"string\",\"optional\":false},\"optional\":true,\"field\":\"enumValues\"}],\"optional\":false,\"name\":\"io.debezium.connector.schema.Column\",\"version\":1},\"optional\":false,\"field\":\"columns\"},{\"type\":\"string\",\"optional\":true,\"field\":\"comment\"}],\"optional\":true,\"name\":\"io.debezium.connector.schema.Table\",\"version\":1,\"field\":\"table\"}],\"optional\":false,\"name\":\"io.debezium.connector.schema.Change\",\"version\":1},\"optional\":false,\"field\":\"tableChanges\"}],\"optional\":false,\"name\":\"io.debezium.connector.mysql.SchemaChangeValue\",\"version\":1},\"payload\":{\"source\":{\"version\":\"2.5.4.Final\",\"connector\":\"mysql\",\"name\":\"normal\",\"ts_ms\":1715153323000,\"snapshot\":\"false\",\"db\":\"wdl_test\",\"sequence\":null,\"table\":\"normal_time\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000070\",\"pos\":6710,\"row\":0,\"thread\":null,\"query\":null},\"ts_ms\":1715153323606,\"databaseName\":\"wdl_test\",\"schemaName\":null,\"ddl\":\"alter table normal_time drop column date_test\",\"tableChanges\":[{\"type\":\"ALTER\",\"id\":\"\\\"wdl_test\\\".\\\"normal_time\\\"\",\"table\":{\"defaultCharsetName\":\"utf8mb4\",\"primaryKeyColumnNames\":[],\"columns\":[{\"name\":\"id\",\"jdbcType\":4,\"nativeType\":null,\"typeName\":\"INT\",\"typeExpression\":\"INT\",\"charsetName\":null,\"length\":null,\"scale\":null,\"position\":1,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"defaultValueExpression\":null,\"enumValues\":null},{\"name\":\"timestamp_test\",\"jdbcType\":2014,\"nativeType\":null,\"typeName\":\"TIMESTAMP\",\"typeExpression\":\"TIMESTAMP\",\"charsetName\":null,\"length\":null,\"scale\":null,\"position\":2,\"optional\":false,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"defaultValueExpression\":\"1970-01-01 00:00:00\",\"enumValues\":null},{\"name\":\"datetime_test\",\"jdbcType\":93,\"nativeType\":null,\"typeName\":\"DATETIME\",\"typeExpression\":\"DATETIME\",\"charsetName\":null,\"length\":null,\"scale\":null,\"position\":3,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"defaultValueExpression\":null,\"enumValues\":null},{\"name\":\"time_test\",\"jdbcType\":92,\"nativeType\":null,\"typeName\":\"TIME\",\"typeExpression\":\"TIME\",\"charsetName\":null,\"length\":null,\"scale\":null,\"position\":4,\"optional\":true,\"autoIncremented\":false,\"generated\":false,\"comment\":null,\"defaultValueExpression\":\"12:00\",\"enumValues\":null}],\"comment\":null}}]}}";
        SchemaAndValue schemaAndValue =
                jsonConverter.toConnectData(topic, alterTopicMsg.getBytes(StandardCharsets.UTF_8));
        SinkRecord sinkRecord =
                TestRecordBuffer.newSinkRecord(schemaAndValue.value(), 8, schemaAndValue.schema());
        String normalTimeSchemaStr =
                "{\"keysType\":\"UNIQUE_KEYS\",\"properties\":[{\"name\":\"id\",\"aggregation_type\":\"\",\"comment\":\"\",\"type\":\"INT\"},{\"name\":\"timestamp_test\",\"aggregation_type\":\"NONE\",\"comment\":\"\",\"type\":\"DATETIMEV2\"},{\"name\":\"datetime_test\",\"aggregation_type\":\"NONE\",\"comment\":\"\",\"type\":\"DATETIMEV2\"},{\"name\":\"date_test\",\"aggregation_type\":\"NONE\",\"comment\":\"\",\"type\":\"DATEV2\"}],\"status\":200}";
        Schema normalTimeSchema = null;
        try {
            normalTimeSchema = objectMapper.readValue(normalTimeSchemaStr, Schema.class);
        } catch (JsonProcessingException e) {
            throw new DorisException(e);
        }
        mockRestService
                .when(() -> RestService.getSchema(any(), any(), any(), any()))
                .thenReturn(normalTimeSchema);

        debeziumSchemaChange.insert(sinkRecord);
        List<String> ddlSqlList = debeziumSchemaChange.getDdlSqlList();
        Assert.assertEquals(
                ddlSqlList.get(0),
                "ALTER TABLE `test_db`.`normal_time` ADD COLUMN `time_test` STRING DEFAULT '12:00'");
        Assert.assertEquals(
                ddlSqlList.get(1), "ALTER TABLE `test_db`.`normal_time` DROP COLUMN `date_test`");
    }

    @After
    public void close() {
        mockRestService.close();
    }
}
