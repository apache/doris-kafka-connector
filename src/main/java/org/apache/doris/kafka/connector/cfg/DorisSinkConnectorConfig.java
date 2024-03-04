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

import java.time.Duration;
import java.util.Map;
import org.apache.doris.kafka.connector.DorisSinkConnector;
import org.apache.doris.kafka.connector.utils.ConfigCheckUtils;
import org.apache.doris.kafka.connector.writer.DeliveryGuarantee;
import org.apache.doris.kafka.connector.writer.load.LoadModel;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Doris SinkConnectorConfig */
public class DorisSinkConnectorConfig {
    private static final Logger LOG = LoggerFactory.getLogger(DorisSinkConnector.class);

    public static final String NAME = "name";
    public static final String TOPICS = "topics";
    public static final String TOPICS_REGEX = "topics.regex";

    // Connector config
    private static final String CONNECTOR_CONFIG = "Connector Config";
    public static final String BUFFER_COUNT_RECORDS = "buffer.count.records";
    public static final long BUFFER_COUNT_RECORDS_DEFAULT = 10000;
    public static final String BUFFER_SIZE_BYTES = "buffer.size.bytes";
    public static final long BUFFER_SIZE_BYTES_DEFAULT = 5000000;
    public static final long BUFFER_SIZE_BYTES_MIN = 1;
    public static final String TOPICS_TABLES_MAP = "doris.topic2table.map";
    public static final String LABEL_PREFIX = "label.prefix";

    // Time in seconds
    public static final long BUFFER_FLUSH_TIME_SEC_MIN = 10;
    public static final long BUFFER_FLUSH_TIME_SEC_DEFAULT = 120;
    public static final String BUFFER_FLUSH_TIME_SEC = "buffer.flush.time";

    private static final String DORIS_INFO = "Doris Info";

    // doris config
    public static final String DORIS_URLS = "doris.urls";
    public static final String DORIS_QUERY_PORT = "doris.query.port";
    public static final String DORIS_HTTP_PORT = "doris.http.port";
    public static final String DORIS_USER = "doris.user";
    public static final String DORIS_PASSWORD = "doris.password";
    public static final String DORIS_DATABASE = "doris.database";
    public static final String REQUEST_READ_TIMEOUT_MS = "request.read.timeout.ms";
    public static final String REQUEST_CONNECT_TIMEOUT_MS = "request.connect.timeout.ms";
    public static final Integer DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT = 30 * 1000;
    public static final Integer DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT = 30 * 1000;
    public static final String LOAD_MODEL = "load.model";
    public static final String LOAD_MODEL_DEFAULT = LoadModel.STREAM_LOAD.name();
    public static final String AUTO_REDIRECT = "auto.redirect";
    public static final String DELIVERY_GUARANTEE = "delivery.guarantee";
    public static final String DELIVERY_GUARANTEE_DEFAULT = DeliveryGuarantee.AT_LEAST_ONCE.name();
    // Prefix for Doris StreamLoad specific properties.
    public static final String STREAM_LOAD_PROP_PREFIX = "sink.properties.";

    // metrics
    public static final String JMX_OPT = "jmx";
    public static final boolean JMX_OPT_DEFAULT = true;

    public static final String ENABLE_DELETE = "enable.delete";
    public static final boolean ENABLE_DELETE_DEFAULT = false;

    private static final ConfigDef.Validator nonEmptyStringValidator =
            new ConfigDef.NonEmptyString();
    private static final ConfigDef.Validator topicToTableValidator = new TopicToTableValidator();

    public static void setDefaultValues(Map<String, String> config) {
        setFieldToDefaultValues(config, BUFFER_COUNT_RECORDS, BUFFER_COUNT_RECORDS_DEFAULT);
        setFieldToDefaultValues(config, BUFFER_SIZE_BYTES, BUFFER_SIZE_BYTES_DEFAULT);
        setFieldToDefaultValues(config, BUFFER_FLUSH_TIME_SEC, BUFFER_FLUSH_TIME_SEC_DEFAULT);
    }

    static void setFieldToDefaultValues(Map<String, String> config, String field, Long value) {
        if (!config.containsKey(field)) {
            config.put(field, value + "");
            LOG.info("{} set to default {} seconds", field, value);
        }
    }

    public static ConfigDef newConfigDef() {
        return new ConfigDef()
                .define(
                        DORIS_URLS,
                        Type.STRING,
                        null,
                        nonEmptyStringValidator,
                        Importance.HIGH,
                        "Doris account url",
                        DORIS_INFO,
                        0,
                        ConfigDef.Width.NONE,
                        DORIS_URLS)
                .define(
                        DORIS_QUERY_PORT,
                        Type.INT,
                        Importance.HIGH,
                        "Doris query port",
                        DORIS_INFO,
                        1,
                        ConfigDef.Width.NONE,
                        DORIS_URLS)
                .define(
                        DORIS_HTTP_PORT,
                        Type.INT,
                        Importance.HIGH,
                        "Doris http port",
                        DORIS_INFO,
                        2,
                        ConfigDef.Width.NONE,
                        DORIS_HTTP_PORT)
                .define(
                        DORIS_USER,
                        Type.STRING,
                        null,
                        nonEmptyStringValidator,
                        Importance.HIGH,
                        "Doris user name",
                        DORIS_INFO,
                        3,
                        ConfigDef.Width.NONE,
                        DORIS_USER)
                .define(
                        DORIS_PASSWORD,
                        Type.PASSWORD,
                        "",
                        Importance.HIGH,
                        "Doris password",
                        DORIS_INFO,
                        4,
                        ConfigDef.Width.NONE,
                        DORIS_PASSWORD)
                .define(
                        DORIS_DATABASE,
                        Type.STRING,
                        null,
                        Importance.HIGH,
                        "Doris database name",
                        DORIS_INFO,
                        6,
                        ConfigDef.Width.NONE,
                        DORIS_DATABASE)
                .define(
                        TOPICS_TABLES_MAP,
                        Type.STRING,
                        "",
                        topicToTableValidator,
                        Importance.LOW,
                        "Map of topics to tables (optional). Format : comma-separated tuples, e.g."
                                + " <topic-1>:<table-1>,<topic-2>:<table-2>,... ",
                        CONNECTOR_CONFIG,
                        0,
                        ConfigDef.Width.NONE,
                        TOPICS_TABLES_MAP)
                .define(
                        BUFFER_COUNT_RECORDS,
                        Type.LONG,
                        BUFFER_COUNT_RECORDS_DEFAULT,
                        ConfigDef.Range.atLeast(1),
                        Importance.LOW,
                        "Number of records buffered in memory per partition before triggering",
                        CONNECTOR_CONFIG,
                        1,
                        ConfigDef.Width.NONE,
                        BUFFER_COUNT_RECORDS)
                .define(
                        BUFFER_SIZE_BYTES,
                        Type.LONG,
                        BUFFER_SIZE_BYTES_DEFAULT,
                        ConfigDef.Range.atLeast(1),
                        Importance.LOW,
                        "Cumulative size of records buffered in memory per partition before triggering",
                        CONNECTOR_CONFIG,
                        2,
                        ConfigDef.Width.NONE,
                        BUFFER_SIZE_BYTES)
                .define(
                        BUFFER_FLUSH_TIME_SEC,
                        Type.LONG,
                        BUFFER_FLUSH_TIME_SEC_DEFAULT,
                        ConfigDef.Range.atLeast(Duration.ofSeconds(1).getSeconds()),
                        Importance.LOW,
                        "The time in seconds to flush cached data",
                        CONNECTOR_CONFIG,
                        3,
                        ConfigDef.Width.NONE,
                        BUFFER_FLUSH_TIME_SEC)
                .define(
                        JMX_OPT,
                        ConfigDef.Type.BOOLEAN,
                        JMX_OPT_DEFAULT,
                        ConfigDef.Importance.HIGH,
                        "Whether to enable JMX MBeans for custom metrics")
                .define(
                        ENABLE_DELETE,
                        ConfigDef.Type.BOOLEAN,
                        ENABLE_DELETE_DEFAULT,
                        ConfigDef.Importance.HIGH,
                        "Used to synchronize delete events")
                .define(
                        LOAD_MODEL,
                        Type.STRING,
                        LOAD_MODEL_DEFAULT,
                        Importance.HIGH,
                        "load model is stream_load.");
    }

    public static class TopicToTableValidator implements ConfigDef.Validator {
        public TopicToTableValidator() {}

        public void ensureValid(String name, Object value) {
            String s = (String) value;
            if (s != null && !s.isEmpty()) // this value is optional and can be empty
            {
                if (ConfigCheckUtils.parseTopicToTableMap(s) == null) {
                    throw new ConfigException(
                            name, value, "Format: <topic-1>:<table-1>,<topic-2>:<table-2>,...");
                }
            }
        }

        public String toString() {
            return "Topic to table map format : comma-separated tuples, e.g."
                    + " <topic-1>:<table-1>,<topic-2>:<table-2>,... ";
        }
    }
}
