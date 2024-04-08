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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import org.apache.doris.kafka.connector.cfg.DorisOptions;
import org.apache.doris.kafka.connector.exception.DataFormatException;
import org.apache.doris.kafka.connector.writer.LoadConstants;
import org.apache.doris.kafka.connector.writer.RecordBuffer;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordService {
    private static final Logger LOG = LoggerFactory.getLogger(RecordService.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final JsonConverter converter;
    private DorisOptions dorisOptions;

    public RecordService() {
        this.converter = new JsonConverter();
        Map<String, Object> converterConfig = new HashMap<>();
        converterConfig.put("schemas.enable", "false");
        this.converter.configure(converterConfig, false);
    }

    public RecordService(DorisOptions dorisOptions) {
        this();
        this.dorisOptions = dorisOptions;
    }

    /**
     * process struct record from debezium: { "schema": { "type": "struct", "fields": [ ...... ],
     * "optional": false, "name": "" }, "payload": { "name": "doris", "__deleted": "true" } }
     */
    public String processStructRecord(SinkRecord record) {
        byte[] bytes =
                converter.fromConnectData(record.topic(), record.valueSchema(), record.value());
        String recordValue = new String(bytes, StandardCharsets.UTF_8);
        try {
            Map<String, Object> recordMap =
                    MAPPER.readValue(recordValue, new TypeReference<Map<String, Object>>() {});
            if (ConverterMode.DEBEZIUM_INGESTION == dorisOptions.getConverterMode()) {
                // delete sign sync
                if ("d".equals(recordMap.get("op"))) {
                    Map<String, Object> beforeValue = (Map<String, Object>) recordMap.get("before");
                    beforeValue.put(LoadConstants.DORIS_DELETE_SIGN, LoadConstants.DORIS_DEL_TRUE);
                    return MAPPER.writeValueAsString(beforeValue);
                }
                Map<String, Object> afterValue = (Map<String, Object>) recordMap.get("after");
                afterValue.put(LoadConstants.DORIS_DELETE_SIGN, LoadConstants.DORIS_DEL_FALSE);
                return MAPPER.writeValueAsString(afterValue);
            }
        } catch (JsonProcessingException e) {
            LOG.error("parse record failed, cause by parse json error: {}", recordValue);
        }
        return recordValue;
    }

    /** process list record from kafka [{"name":"doris1"},{"name":"doris2"}] */
    public String processListRecord(SinkRecord record) {
        try {
            StringJoiner sj = new StringJoiner(RecordBuffer.LINE_SEPARATOR);
            List recordList = (List) record.value();
            for (Object item : recordList) {
                sj.add(MAPPER.writeValueAsString(item));
            }
            return sj.toString();
        } catch (IOException e) {
            LOG.error("process list record failed: {}", record.value());
            throw new DataFormatException("process list record failed");
        }
    }

    /** process map record from kafka {"name":"doris"} */
    public String processMapRecord(SinkRecord record) {
        try {
            return MAPPER.writeValueAsString(record.value());
        } catch (IOException e) {
            LOG.error("process map record failed: {}", record.value());
            throw new DataFormatException("process map record failed");
        }
    }

    /** If not struct, map, list, use the default string */
    public String processStringRecord(SinkRecord record) {
        return record.value().toString();
    }

    /**
     * Given a single Record from put API, process it and convert it into a Json String.
     *
     * @param record record from Kafka
     * @return Json String
     */
    public String getProcessedRecord(SinkRecord record) {
        if (record.value() instanceof Struct) {
            return processStructRecord(record);
        } else if (record.value() instanceof List) {
            return processListRecord(record);
        } else if (record.value() instanceof Map) {
            return processMapRecord(record);
        } else {
            return processStringRecord(record);
        }
    }
}
