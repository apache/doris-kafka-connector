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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.doris.kafka.connector.writer.TestRecordBuffer;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Test;

public class TestRecordService {

    private RecordService recordService = new RecordService();

    @Test
    public void processStructRecord() {
        JsonConverter jsonConverter = new JsonConverter();
        HashMap<String, String> config = new HashMap<>();
        jsonConverter.configure(config, false);
        // no delete value
        String nodelete =
                "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"age\"},{\"type\":\"string\",\"optional\":true,\"field\":\"__deleted\"}],\"optional\":false,\"name\":\"mysql_master.test.test_flink.Value\"},\"payload\":{\"name\":\"zhangsan\",\"age\":1,\"__deleted\":\"false\"}}";
        SchemaAndValue nodeleteValue =
                jsonConverter.toConnectData("test", nodelete.getBytes(StandardCharsets.UTF_8));
        SinkRecord record =
                TestRecordBuffer.newSinkRecord(nodeleteValue.value(), 1, nodeleteValue.schema());
        String s = recordService.processStructRecord(record);
        Assert.assertEquals("{\"name\":\"zhangsan\",\"age\":1,\"__DORIS_DELETE_SIGN__\":\"0\"}", s);

        // delete value
        String delete =
                "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"age\"},{\"type\":\"string\",\"optional\":true,\"field\":\"__deleted\"}],\"optional\":false,\"name\":\"mysql_master.test.test_flink.Value\"},\"payload\":{\"name\":\"zhangsan\",\"age\":1,\"__deleted\":\"true\"}}";
        SchemaAndValue deleteValue =
                jsonConverter.toConnectData("test", delete.getBytes(StandardCharsets.UTF_8));
        SinkRecord record2 =
                TestRecordBuffer.newSinkRecord(deleteValue.value(), 1, deleteValue.schema());
        String s2 = recordService.processStructRecord(record2);
        Assert.assertEquals(
                "{\"name\":\"zhangsan\",\"age\":1,\"__DORIS_DELETE_SIGN__\":\"1\"}", s2);

        // no setting delete sign, not set transforms.unwrap.delete.handling.mode=rewrite
        String deletenone =
                "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"age\"}],\"optional\":false,\"name\":\"mysql_master.test.test_flink.Value\"},\"payload\":{\"name\":\"zhangsan\",\"age\":1}}";
        SchemaAndValue deletenoneValue =
                jsonConverter.toConnectData("test", deletenone.getBytes(StandardCharsets.UTF_8));
        SinkRecord record3 =
                TestRecordBuffer.newSinkRecord(
                        deletenoneValue.value(), 1, deletenoneValue.schema());
        String s3 = recordService.processStructRecord(record3);
        Assert.assertEquals(
                "{\"name\":\"zhangsan\",\"age\":1,\"__DORIS_DELETE_SIGN__\":\"0\"}", s3);
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
