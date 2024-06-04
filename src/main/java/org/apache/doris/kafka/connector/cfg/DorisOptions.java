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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.doris.kafka.connector.converter.ConverterMode;
import org.apache.doris.kafka.connector.converter.schema.SchemaEvolutionMode;
import org.apache.doris.kafka.connector.utils.ConfigCheckUtils;
import org.apache.doris.kafka.connector.writer.DeliveryGuarantee;
import org.apache.doris.kafka.connector.writer.load.LoadModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DorisOptions {
    private static final Logger LOG = LoggerFactory.getLogger(DorisOptions.class);
    private final String name;
    private final String urls;
    private final int queryPort;
    private final int httpPort;
    private final String user;
    private final String password;
    private final String database;
    private final Map<String, String> topicMap;
    private final int fileSize;
    private final int recordNum;
    private long flushTime;
    private boolean enableCustomJMX;
    private final int taskId;
    private final boolean enableDelete;
    private boolean autoRedirect = true;
    private int requestReadTimeoutMs;
    private int requestConnectTimeoutMs;
    /** Properties for the StreamLoad. */
    private final Properties streamLoadProp = new Properties();

    private String labelPrefix;
    private String databaseTimeZone;
    private LoadModel loadModel;
    private DeliveryGuarantee deliveryGuarantee;
    private ConverterMode converterMode;
    private SchemaEvolutionMode schemaEvolutionMode;

    public DorisOptions(Map<String, String> config) {
        this.name = config.get(DorisSinkConnectorConfig.NAME);
        this.urls = config.get(DorisSinkConnectorConfig.DORIS_URLS);
        this.queryPort = Integer.parseInt(config.get(DorisSinkConnectorConfig.DORIS_QUERY_PORT));
        this.httpPort = Integer.parseInt(config.get(DorisSinkConnectorConfig.DORIS_HTTP_PORT));
        this.user = config.get(DorisSinkConnectorConfig.DORIS_USER);
        this.password = config.get(DorisSinkConnectorConfig.DORIS_PASSWORD);
        this.database = config.get(DorisSinkConnectorConfig.DORIS_DATABASE);
        this.taskId = Integer.parseInt(config.get(ConfigCheckUtils.TASK_ID));
        this.databaseTimeZone = DorisSinkConnectorConfig.DATABASE_TIME_ZONE_DEFAULT;
        if (config.containsKey(DorisSinkConnectorConfig.DATABASE_TIME_ZONE)) {
            this.databaseTimeZone = config.get(DorisSinkConnectorConfig.DATABASE_TIME_ZONE);
        }
        this.loadModel =
                LoadModel.of(
                        config.getOrDefault(
                                DorisSinkConnectorConfig.LOAD_MODEL,
                                DorisSinkConnectorConfig.LOAD_MODEL_DEFAULT));
        this.deliveryGuarantee =
                DeliveryGuarantee.of(
                        config.getOrDefault(
                                DorisSinkConnectorConfig.DELIVERY_GUARANTEE,
                                DorisSinkConnectorConfig.DELIVERY_GUARANTEE_DEFAULT));
        this.converterMode =
                ConverterMode.of(
                        config.getOrDefault(
                                DorisSinkConnectorConfig.CONVERT_MODE,
                                DorisSinkConnectorConfig.CONVERT_MODE_DEFAULT));
        this.schemaEvolutionMode =
                SchemaEvolutionMode.of(
                        config.getOrDefault(
                                DorisSinkConnectorConfig.DEBEZIUM_SCHEMA_EVOLUTION,
                                DorisSinkConnectorConfig.DEBEZIUM_SCHEMA_EVOLUTION_DEFAULT));

        this.fileSize = Integer.parseInt(config.get(DorisSinkConnectorConfig.BUFFER_SIZE_BYTES));
        this.recordNum =
                Integer.parseInt(config.get(DorisSinkConnectorConfig.BUFFER_COUNT_RECORDS));

        this.flushTime = Long.parseLong(config.get(DorisSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC));
        if (flushTime < DorisSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_MIN) {
            LOG.warn(
                    "flush time is {} seconds, it is smaller than the minimum flush time {} seconds, reset to the minimum flush time",
                    flushTime,
                    DorisSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_MIN);
            this.flushTime = DorisSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_MIN;
        }
        this.topicMap = getTopicToTableMap(config);

        enableCustomJMX = DorisSinkConnectorConfig.JMX_OPT_DEFAULT;
        if (config.containsKey(DorisSinkConnectorConfig.JMX_OPT)) {
            enableCustomJMX = Boolean.parseBoolean(config.get(DorisSinkConnectorConfig.JMX_OPT));
        }
        enableDelete = Boolean.parseBoolean(config.get(DorisSinkConnectorConfig.ENABLE_DELETE));
        this.requestConnectTimeoutMs =
                DorisSinkConnectorConfig.DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT;
        this.requestReadTimeoutMs = DorisSinkConnectorConfig.DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT;
        this.labelPrefix = config.get(DorisSinkConnectorConfig.NAME);
        if (config.containsKey(DorisSinkConnectorConfig.LABEL_PREFIX)) {
            this.labelPrefix = config.get(DorisSinkConnectorConfig.LABEL_PREFIX);
        }
        if (config.containsKey(DorisSinkConnectorConfig.AUTO_REDIRECT)) {
            this.autoRedirect =
                    Boolean.parseBoolean(config.get(DorisSinkConnectorConfig.AUTO_REDIRECT));
        }
        if (config.containsKey(DorisSinkConnectorConfig.REQUEST_CONNECT_TIMEOUT_MS)) {
            this.requestConnectTimeoutMs =
                    Integer.parseInt(
                            config.get(DorisSinkConnectorConfig.REQUEST_CONNECT_TIMEOUT_MS));
        }
        if (config.containsKey(DorisSinkConnectorConfig.REQUEST_READ_TIMEOUT_MS)) {
            this.requestReadTimeoutMs =
                    Integer.parseInt(config.get(DorisSinkConnectorConfig.REQUEST_READ_TIMEOUT_MS));
        }
        getStreamLoadPropFromConfig(config);
    }

    private void getStreamLoadPropFromConfig(Map<String, String> config) {
        setStreamLoadDefaultValues();
        for (Map.Entry<String, String> entry : config.entrySet()) {
            if (entry.getKey().startsWith(DorisSinkConnectorConfig.STREAM_LOAD_PROP_PREFIX)) {
                String subKey =
                        entry.getKey()
                                .substring(
                                        DorisSinkConnectorConfig.STREAM_LOAD_PROP_PREFIX.length());
                streamLoadProp.put(subKey, entry.getValue());
            }
        }
    }

    private void setStreamLoadDefaultValues() {
        streamLoadProp.setProperty("format", "json");
        streamLoadProp.setProperty("read_json_by_line", "true");
    }

    public String getName() {
        return name;
    }

    public String getUrls() {
        return urls;
    }

    public int getQueryPort() {
        return queryPort;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getDatabase() {
        return database;
    }

    public int getFileSize() {
        return fileSize;
    }

    public int getRecordNum() {
        return recordNum;
    }

    public long getFlushTime() {
        return flushTime;
    }

    public String getTopicMapTable(String topic) {
        return topicMap.get(topic);
    }

    public Map<String, String> getTopicMap() {
        return topicMap;
    }

    public String getQueryUrl() {
        List<String> queryUrls = getQueryUrls();
        return queryUrls.get(0);
    }

    public String getHttpUrl() {
        List<String> httpUrls = getHttpUrls();
        return httpUrls.get(0);
    }

    public List<String> getQueryUrls() {
        List<String> queryUrls = new ArrayList<>();
        if (urls.contains(",")) {
            queryUrls =
                    Arrays.stream(urls.split(","))
                            .map(
                                    url -> {
                                        return url.trim() + ":" + queryPort;
                                    })
                            .collect(Collectors.toList());
            Collections.shuffle(queryUrls);
            return queryUrls;
        }
        queryUrls.add(urls + ":" + queryPort);
        return queryUrls;
    }

    public List<String> getHttpUrls() {
        List<String> httpUrls = new ArrayList<>();
        if (urls.contains(",")) {
            httpUrls =
                    Arrays.stream(urls.split(","))
                            .map(
                                    url -> {
                                        return url.trim() + ":" + httpPort;
                                    })
                            .collect(Collectors.toList());
            Collections.shuffle(httpUrls);
            return httpUrls;
        }
        httpUrls.add(urls + ":" + httpPort);
        return httpUrls;
    }

    public Integer getRequestReadTimeoutMs() {
        return this.requestReadTimeoutMs;
    }

    public Integer getRequestConnectTimeoutMs() {
        return this.requestConnectTimeoutMs;
    }

    public Properties getStreamLoadProp() {
        return streamLoadProp;
    }

    public String getLabelPrefix() {
        return this.labelPrefix;
    }

    public boolean isEnableCustomJMX() {
        return enableCustomJMX;
    }

    public int getTaskId() {
        return taskId;
    }

    public LoadModel getLoadModel() {
        return loadModel;
    }

    public DeliveryGuarantee getDeliveryGuarantee() {
        return this.deliveryGuarantee;
    }

    public ConverterMode getConverterMode() {
        return this.converterMode;
    }

    public SchemaEvolutionMode getSchemaEvolutionMode() {
        return this.schemaEvolutionMode;
    }

    public boolean isAutoRedirect() {
        return autoRedirect;
    }

    public String getDatabaseTimeZone() {
        return databaseTimeZone;
    }

    public boolean isEnableDelete() {
        return enableDelete;
    }

    /**
     * parse topic to table map
     *
     * @param config connector config file
     * @return result map
     */
    static Map<String, String> getTopicToTableMap(Map<String, String> config) {
        if (config.containsKey(DorisSinkConnectorConfig.TOPICS_TABLES_MAP)) {
            Map<String, String> result =
                    ConfigCheckUtils.parseTopicToTableMap(
                            config.get(DorisSinkConnectorConfig.TOPICS_TABLES_MAP));
            if (result != null) {
                return result;
            }
        }
        return new HashMap<>();
    }

    @Override
    public String toString() {
        return "DorisOptions{"
                + "name='"
                + name
                + '\''
                + ", urls='"
                + urls
                + '\''
                + ", queryPort="
                + queryPort
                + ", httpPort="
                + httpPort
                + ", user='"
                + user
                + '\''
                + ", password='"
                + password
                + '\''
                + ", database='"
                + database
                + '\''
                + ", topicMap="
                + topicMap
                + ", fileSize="
                + fileSize
                + ", recordNum="
                + recordNum
                + ", flushTime="
                + flushTime
                + ", enableCustomJMX="
                + enableCustomJMX
                + ", taskId="
                + taskId
                + ", enableDelete="
                + enableDelete
                + ", autoRedirect="
                + autoRedirect
                + ", requestReadTimeoutMs="
                + requestReadTimeoutMs
                + ", requestConnectTimeoutMs="
                + requestConnectTimeoutMs
                + ", streamLoadProp="
                + streamLoadProp
                + ", labelPrefix='"
                + labelPrefix
                + '\''
                + ", loadModel="
                + loadModel
                + ", deliveryGuarantee="
                + deliveryGuarantee
                + '}';
    }
}
