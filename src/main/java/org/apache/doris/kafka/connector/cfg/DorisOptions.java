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
    private final String tableField;
    private final int fileSize;
    private final int recordNum;
    private final long flushTime;
    private final boolean enableCustomJMX;
    private final int taskId;
    private final boolean enableDelete;
    private boolean enable2PC = true;
    private boolean force2PC;
    private boolean autoRedirect = true;
    private int requestReadTimeoutMs;
    private int requestConnectTimeoutMs;
    private boolean enableGroupCommit;
    /** Properties for the StreamLoad. */
    private final Properties streamLoadProp;

    @Deprecated private String labelPrefix;
    private final String databaseTimeZone;
    private final LoadModel loadModel;
    private final DeliveryGuarantee deliveryGuarantee;
    private final ConverterMode converterMode;
    private final SchemaEvolutionMode schemaEvolutionMode;

    public DorisOptions(Map<String, String> config) {
        this.name = config.get(DorisSinkConnectorConfig.NAME);
        this.urls = config.get(DorisSinkConnectorConfig.DORIS_URLS);
        this.queryPort = Integer.parseInt(config.get(DorisSinkConnectorConfig.DORIS_QUERY_PORT));
        this.httpPort = Integer.parseInt(config.get(DorisSinkConnectorConfig.DORIS_HTTP_PORT));
        this.user = config.get(DorisSinkConnectorConfig.DORIS_USER);
        this.password = config.get(DorisSinkConnectorConfig.DORIS_PASSWORD);
        this.database = config.get(DorisSinkConnectorConfig.DORIS_DATABASE);
        this.taskId = Integer.parseInt(config.get(ConfigCheckUtils.TASK_ID));
        this.databaseTimeZone = config.get(DorisSinkConnectorConfig.DATABASE_TIME_ZONE);
        this.loadModel = LoadModel.of(config.get(DorisSinkConnectorConfig.LOAD_MODEL));
        this.deliveryGuarantee =
                DeliveryGuarantee.of(config.get(DorisSinkConnectorConfig.DELIVERY_GUARANTEE));
        this.converterMode = ConverterMode.of(config.get(DorisSinkConnectorConfig.CONVERTER_MODE));
        this.schemaEvolutionMode =
                SchemaEvolutionMode.of(
                        config.get(DorisSinkConnectorConfig.DEBEZIUM_SCHEMA_EVOLUTION));
        this.fileSize = Integer.parseInt(config.get(DorisSinkConnectorConfig.BUFFER_SIZE_BYTES));
        this.recordNum =
                Integer.parseInt(config.get(DorisSinkConnectorConfig.BUFFER_COUNT_RECORDS));

        this.flushTime = Long.parseLong(config.get(DorisSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC));
        this.topicMap =
                ConfigCheckUtils.parseTopicToTableMap(
                        config.get(DorisSinkConnectorConfig.TOPICS_TABLES_MAP));
        this.tableField = config.get(DorisSinkConnectorConfig.DORIS_TABLE_FIELD);

        if (config.containsKey(DorisSinkConnectorConfig.ENABLE_2PC)) {
            if (Boolean.parseBoolean(config.get(DorisSinkConnectorConfig.ENABLE_2PC))) {
                this.enable2PC = true;
                this.force2PC = true;
            } else {
                this.enable2PC = false;
            }
        }
        this.enableCustomJMX = Boolean.parseBoolean(config.get(DorisSinkConnectorConfig.JMX_OPT));
        this.enableDelete =
                Boolean.parseBoolean(config.get(DorisSinkConnectorConfig.ENABLE_DELETE));
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
        this.streamLoadProp = getStreamLoadPropFromConfig(config);
        this.enableGroupCommit = ConfigCheckUtils.validateGroupCommitMode(this);
    }

    private Properties getStreamLoadPropFromConfig(Map<String, String> config) {
        Properties properties = new Properties();
        properties.putAll(getStreamLoadDefaultValues());
        for (Map.Entry<String, String> entry : config.entrySet()) {
            if (entry.getKey().startsWith(DorisSinkConnectorConfig.STREAM_LOAD_PROP_PREFIX)) {
                String subKey =
                        entry.getKey()
                                .substring(
                                        DorisSinkConnectorConfig.STREAM_LOAD_PROP_PREFIX.length());
                properties.put(subKey, entry.getValue());
            }
        }
        return properties;
    }

    private Properties getStreamLoadDefaultValues() {
        Properties properties = new Properties();
        properties.setProperty("format", "json");
        properties.setProperty("read_json_by_line", "true");
        return properties;
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

    public String getTableField() {
        return tableField;
    }

    public boolean enable2PC() {
        return enable2PC;
    }

    public boolean force2PC() {
        return force2PC;
    }

    public void setEnable2PC(boolean enable2PC) {
        this.enable2PC = enable2PC;
    }

    public boolean enableGroupCommit() {
        return enableGroupCommit;
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
}
