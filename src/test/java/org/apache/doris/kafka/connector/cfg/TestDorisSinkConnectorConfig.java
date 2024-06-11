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

package org.apache.doris.kafka.connector.cfg;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.doris.kafka.connector.exception.DorisException;
import org.apache.doris.kafka.connector.utils.ConfigCheckUtils;
import org.junit.Assert;
import org.junit.Test;

public class TestDorisSinkConnectorConfig {

    public static Map<String, String> getConfig() {
        Map<String, String> config = new HashMap<>();
        config.put(DorisSinkConnectorConfig.NAME, "test");
        config.put(DorisSinkConnectorConfig.TOPICS, "topic1,topic2");
        config.put(DorisSinkConnectorConfig.DORIS_QUERY_PORT, "8090");
        config.put(DorisSinkConnectorConfig.DORIS_HTTP_PORT, "8080");

        config.put(DorisSinkConnectorConfig.DORIS_URLS, "https://doris.apache.org");
        config.put(DorisSinkConnectorConfig.DORIS_USER, "userName");
        config.put(DorisSinkConnectorConfig.DORIS_PASSWORD, "password");

        config.put(DorisSinkConnectorConfig.DORIS_DATABASE, "testDatabase");
        config.put(
                DorisSinkConnectorConfig.BUFFER_COUNT_RECORDS,
                DorisSinkConnectorConfig.BUFFER_COUNT_RECORDS_DEFAULT + "");
        config.put(
                DorisSinkConnectorConfig.BUFFER_SIZE_BYTES,
                DorisSinkConnectorConfig.BUFFER_SIZE_BYTES_DEFAULT + "");
        config.put(
                DorisSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC,
                DorisSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_DEFAULT + "");
        DorisSinkConnectorConfig.setDefaultValues(config);
        return config;
    }

    @Test
    public void testConfig() {
        Map<String, String> config = getConfig();
        ConfigCheckUtils.validateConfig(config);
    }

    @Test(expected = DorisException.class)
    public void testEmptyFlushTime() {
        Map<String, String> config = getConfig();
        config.remove(DorisSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC);
        ConfigCheckUtils.validateConfig(config);
    }

    @Test(expected = DorisException.class)
    public void testFlushTimeSmall() {
        Map<String, String> config = getConfig();
        config.put(
                DorisSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC,
                (DorisSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_MIN - 1) + "");
        ConfigCheckUtils.validateConfig(config);
    }

    @Test(expected = DorisException.class)
    public void testFlushTimeNotNumber() {
        Map<String, String> config = getConfig();
        config.put(DorisSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC, "fdas");
        ConfigCheckUtils.validateConfig(config);
    }

    @Test(expected = DorisException.class)
    public void testAutoRedirectException() {
        Map<String, String> config = getConfig();
        config.put(DorisSinkConnectorConfig.AUTO_REDIRECT, "abc");
        ConfigCheckUtils.validateConfig(config);
    }

    @Test
    public void testAutoRedirect() {
        Map<String, String> config = getConfig();
        ConfigCheckUtils.validateConfig(config);

        config.put(DorisSinkConnectorConfig.AUTO_REDIRECT, "true");
        ConfigCheckUtils.validateConfig(config);

        config.put(DorisSinkConnectorConfig.AUTO_REDIRECT, "false");
        ConfigCheckUtils.validateConfig(config);
    }

    @Test(expected = DorisException.class)
    public void testEmptyName() {
        Map<String, String> config = getConfig();
        config.remove(DorisSinkConnectorConfig.NAME);
        ConfigCheckUtils.validateConfig(config);
    }

    @Test(expected = DorisException.class)
    public void testEmptyTopics() {
        Map<String, String> config = getConfig();
        config.remove(DorisSinkConnectorConfig.TOPICS);
        ConfigCheckUtils.validateConfig(config);
    }

    @Test(expected = DorisException.class)
    public void testURL() {
        Map<String, String> config = getConfig();
        config.remove(DorisSinkConnectorConfig.DORIS_URLS);
        ConfigCheckUtils.validateConfig(config);
    }

    @Test(expected = DorisException.class)
    public void testEmptyUser() {
        Map<String, String> config = getConfig();
        config.remove(DorisSinkConnectorConfig.DORIS_USER);
        ConfigCheckUtils.validateConfig(config);
    }

    @Test(expected = DorisException.class)
    public void testEmptyQueryPort() {
        Map<String, String> config = getConfig();
        config.remove(DorisSinkConnectorConfig.DORIS_QUERY_PORT);
        ConfigCheckUtils.validateConfig(config);
    }

    @Test(expected = DorisException.class)
    public void testEmptyHttpPort() {
        Map<String, String> config = getConfig();
        config.remove(DorisSinkConnectorConfig.DORIS_HTTP_PORT);
        ConfigCheckUtils.validateConfig(config);
    }

    @Test(expected = DorisException.class)
    public void testIllegalTopicMap() {
        Map<String, String> config = getConfig();
        config.put(DorisSinkConnectorConfig.TOPICS_TABLES_MAP, "$@#$#@%^$12312");
        ConfigCheckUtils.validateConfig(config);
    }

    @Test(expected = DorisException.class)
    public void testDuplicatedTopic() {
        Map<String, String> config = getConfig();
        config.put(DorisSinkConnectorConfig.TOPICS_TABLES_MAP, "topic1:table1,topic1:table2");
        ConfigCheckUtils.validateConfig(config);
    }

    @Test
    public void testDuplicatedTableName() {
        Map<String, String> config = getConfig();
        config.put(DorisSinkConnectorConfig.TOPICS_TABLES_MAP, "topic1:table1,topic2:table1");
        ConfigCheckUtils.validateConfig(config);
    }

    @Test
    public void testNameMapCovered() {
        Map<String, String> config = getConfig();
        config.put(DorisSinkConnectorConfig.TOPICS, "!@#,$%^,test");
        config.put(DorisSinkConnectorConfig.TOPICS_TABLES_MAP, "!@#:table1,$%^:table2");
        ConfigCheckUtils.validateConfig(config);
    }

    @Test(expected = DorisException.class)
    public void testBufferSizeRange() {
        Map<String, String> config = getConfig();
        config.put(
                DorisSinkConnectorConfig.BUFFER_SIZE_BYTES,
                DorisSinkConnectorConfig.BUFFER_SIZE_BYTES_MIN - 1 + "");
        ConfigCheckUtils.validateConfig(config);
    }

    @Test(expected = DorisException.class)
    public void testBufferSizeValue() {
        Map<String, String> config = getConfig();
        config.put(DorisSinkConnectorConfig.BUFFER_SIZE_BYTES, "afdsa");
        ConfigCheckUtils.validateConfig(config);
    }

    @Test(expected = DorisException.class)
    public void testEmptyBufferSize() {
        Map<String, String> config = getConfig();
        config.remove(DorisSinkConnectorConfig.BUFFER_SIZE_BYTES);
        ConfigCheckUtils.validateConfig(config);
    }

    @Test(expected = DorisException.class)
    public void testEmptyBufferCount() {
        Map<String, String> config = getConfig();
        config.remove(DorisSinkConnectorConfig.BUFFER_COUNT_RECORDS);
        ConfigCheckUtils.validateConfig(config);
    }

    @Test(expected = DorisException.class)
    public void testEmptyBufferCountNegative() {
        Map<String, String> config = getConfig();
        config.put(DorisSinkConnectorConfig.BUFFER_COUNT_RECORDS, "-1");
        ConfigCheckUtils.validateConfig(config);
    }

    @Test(expected = DorisException.class)
    public void testBufferCountValue() {
        Map<String, String> config = getConfig();
        config.put(DorisSinkConnectorConfig.BUFFER_COUNT_RECORDS, "11adssadsa");
        ConfigCheckUtils.validateConfig(config);
    }

    @Test(expected = DorisException.class)
    public void testLoadModelException() {
        Map<String, String> config = getConfig();
        config.put(DorisSinkConnectorConfig.LOAD_MODEL, "stream_loada");
        ConfigCheckUtils.validateConfig(config);
    }

    @Test
    public void testLoadModel() {
        Map<String, String> config = getConfig();
        ConfigCheckUtils.validateConfig(config);
    }

    @Test
    public void testDeliveryGuarantee() {
        Map<String, String> config = getConfig();
        ConfigCheckUtils.validateConfig(config);
    }

    @Test(expected = DorisException.class)
    public void testDeliveryGuaranteeException() {
        Map<String, String> config = getConfig();
        config.put(DorisSinkConnectorConfig.DELIVERY_GUARANTEE, "exactly_oncea");
        ConfigCheckUtils.validateConfig(config);
    }

    @Test
    public void testConverterMode() {
        Map<String, String> config = getConfig();
        ConfigCheckUtils.validateConfig(config);
    }

    @Test(expected = DorisException.class)
    public void testConverterModeException() {
        Map<String, String> config = getConfig();
        config.put(DorisSinkConnectorConfig.CONVERTER_MODE, "debezium_ingestiona");
        ConfigCheckUtils.validateConfig(config);
    }

    @Test
    public void testSchemaEvolutionMode() {
        Map<String, String> config = getConfig();
        ConfigCheckUtils.validateConfig(config);
    }

    @Test(expected = DorisException.class)
    public void testSchemaEvolutionModeException() {
        Map<String, String> config = getConfig();
        config.put(DorisSinkConnectorConfig.DEBEZIUM_SCHEMA_EVOLUTION, "nonea");
        ConfigCheckUtils.validateConfig(config);
    }

    @Test
    public void testConvertToLowercase() {
        Map<String, String> config = getConfig();
        config.put("DELIVERY.guarantee", "at_least_once");
        config.put("load.MODEL", "STREAM_LOAD");
        config.put("DORIS.USER", "root");
        config.put("Enable.deLete", "true");
        config.put("doris.http.port", "8030");
        Map<String, String> convertConfig = DorisSinkConnectorConfig.convertToLowercase(config);
        List<String> result =
                Arrays.asList(
                        DorisSinkConnectorConfig.DELIVERY_GUARANTEE,
                        DorisSinkConnectorConfig.LOAD_MODEL,
                        DorisSinkConnectorConfig.DORIS_USER,
                        DorisSinkConnectorConfig.ENABLE_DELETE,
                        DorisSinkConnectorConfig.DORIS_HTTP_PORT);
        for (String s : result) {
            Assert.assertTrue(convertConfig.containsKey(s));
        }
    }
}
