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

package org.apache.doris.kafka.connector.converter.schema;

import static java.net.HttpURLConnection.HTTP_OK;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.kafka.connector.cfg.DorisOptions;
import org.apache.doris.kafka.connector.converter.RecordDescriptor;
import org.apache.doris.kafka.connector.converter.RecordDescriptor.FieldDescriptor;
import org.apache.doris.kafka.connector.converter.type.doris.DorisType;
import org.apache.doris.kafka.connector.exception.SchemaChangeException;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: Improve table creation configuration properties via doris options
public class SchemaCreationManager {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaCreationManager.class);
    private static final String CREATE_DDL = "CREATE TABLE IF NOT EXISTS %s ";
    private static final String SCHEMA_CHANGE_API = "http://%s/api/query/default_cluster/%s";
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final DorisOptions dorisOptions;

    public SchemaCreationManager(DorisOptions dorisOptions) {
        this.dorisOptions = dorisOptions;
    }

    private static String identifier(String name) {
        return "`" + name + "`";
    }

    private static String identProp(String name) {
        return "\"" + name + "\"";
    }

    private void appendList(
            StringBuilder builder,
            String delimiter,
            Collection<String> columnNames,
            Function<String, String> transform) {
        for (Iterator<String> iterator = columnNames.iterator(); iterator.hasNext(); ) {
            builder.append(transform.apply(iterator.next()));
            if (iterator.hasNext()) {
                builder.append(delimiter);
            }
        }
    }

    private void appendLists(
            StringBuilder builder,
            Collection<String> columnNames1,
            Collection<String> columnNames2,
            Function<String, String> transform) {
        appendLists(builder, ", ", columnNames1, columnNames2, transform);
    }

    private void appendLists(
            StringBuilder builder,
            String delimiter,
            Collection<String> columnNames1,
            Collection<String> columnNames2,
            Function<String, String> transform) {
        appendList(builder, delimiter, columnNames1, transform);
        if (!columnNames1.isEmpty() && !columnNames2.isEmpty()) {
            builder.append(delimiter);
        }
        appendList(builder, delimiter, columnNames2, transform);
    }

    public void createTable(String tableName, RecordDescriptor recordDescriptor) {
        try {
            String database = dorisOptions.getDatabase();
            String statementCreateTableDDL =
                    buildCreateTableDDL(database, tableName, recordDescriptor);
            boolean status =
                    execute(tableName, statementCreateTableDDL, dorisOptions.getDatabase());
            LOG.info(
                    "Created missing {} table from {} database, ddl={}, status={}",
                    tableName,
                    database,
                    statementCreateTableDDL,
                    status);
        } catch (Exception e) {
            LOG.warn("Failed to create table {}, cause by:", tableName, e);
            throw new SchemaChangeException(
                    "Failed to create table " + tableName + ", cause by:", e);
        }
    }

    public String buildCreateTableDDL(
            String database, String tableName, RecordDescriptor recordDescriptor) {

        final StringBuilder dmlBuilder = new StringBuilder();

        dmlBuilder.append(
                String.format(CREATE_DDL, identifier(database) + "." + identifier(tableName)));
        dmlBuilder.append("(");

        Map<String, FieldDescriptor> allFields = recordDescriptor.getFields();
        LinkedHashSet<String> keyFieldNames = recordDescriptor.getKeyFieldNames();
        List<String> allFieldNames = recordDescriptor.getNonKeyFieldNames();
        List<String> nonKeyFieldNames = new ArrayList<>(allFieldNames);
        Function<String, String> transform =
                (name) -> {
                    final StringBuilder columnSpec = new StringBuilder();
                    final FieldDescriptor field = allFields.get(name);
                    final org.apache.kafka.connect.data.Schema fieldSchema = field.getSchema();
                    final String columnName = field.getName();
                    final String columnType = field.getTypeName();
                    final String quotedColumnName = identifier(columnName);
                    if (keyFieldNames.contains(columnName) && columnType.equals(DorisType.STRING)) {
                        columnSpec.append(quotedColumnName).append(" ").append("VARCHAR(65533)");
                    } else {
                        columnSpec.append(quotedColumnName).append(" ").append(columnType);
                    }
                    columnSpec.append(fieldSchema.isOptional() ? " NULL" : " NOT NULL");

                    return columnSpec.toString();
                };

        nonKeyFieldNames.removeIf(keyFieldNames::contains);

        appendLists(dmlBuilder, keyFieldNames, nonKeyFieldNames, transform);
        dmlBuilder.append(")");

        if (!keyFieldNames.isEmpty()) {
            String quotedKeys =
                    keyFieldNames.stream()
                            .map(SchemaCreationManager::identifier)
                            .collect(Collectors.joining(", "));
            dmlBuilder.append(String.format(" ENGINE=OLAP UNIQUE KEY (%s) ", quotedKeys));
            dmlBuilder.append(String.format("DISTRIBUTED BY HASH (%s) BUCKETS AUTO", quotedKeys));
        }

        buildCreateTablePropertiesDDL(dmlBuilder);

        dmlBuilder.append(";");

        return dmlBuilder.toString();
    }

    private void buildCreateTablePropertiesDDL(StringBuilder dmlBuilder) {
        Properties tableConfigProp = this.dorisOptions.getTableConfigProp();
        dmlBuilder.append(" PROPERTIES (");
        String tableConfPropString =
                tableConfigProp.entrySet().stream()
                        .filter(
                                e ->
                                        e.getKey() != null
                                                && TableProperty.isAllowed(e.getKey().toString()))
                        .sorted(Comparator.comparing(e -> String.valueOf(e.getKey())))
                        .map(
                                e ->
                                        identProp(e.getKey().toString())
                                                + " = "
                                                + identProp(e.getValue().toString()))
                        .collect(Collectors.joining(", "));
        dmlBuilder.append(tableConfPropString);
        dmlBuilder.append(")");
    }

    private boolean handleSchemaCreation(Map<String, Object> responseMap, String responseEntity) {
        LOG.info(responseEntity);
        String code = responseMap.getOrDefault("code", "-1").toString();
        if (code.equals("0")) {
            return true;
        } else {
            throw new SchemaChangeException("Failed to create table, response: " + responseEntity);
        }
    }

    /** execute sql in doris. */
    private boolean execute(String tableName, String ddl, String database)
            throws IOException, IllegalArgumentException {
        if (StringUtils.isEmpty(ddl)) {
            return false;
        }
        LOG.info("Execute SQL: {}", ddl);
        HttpPost httpPost = buildHttpPost(ddl, database);
        String responseEntity = "";
        Map<String, Object> responseMap = handleResponse(httpPost, responseEntity, tableName);
        return handleSchemaCreation(responseMap, responseEntity);
    }

    public HttpPost buildHttpPost(String ddl, String database)
            throws IllegalArgumentException, IOException {
        Map<String, String> param = new HashMap<>();
        param.put("stmt", ddl);
        String requestUrl = String.format(SCHEMA_CHANGE_API, dorisOptions.getHttpUrl(), database);
        HttpPost httpPost = new HttpPost(requestUrl);
        httpPost.setHeader(HttpHeaders.AUTHORIZATION, authHeader());
        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        httpPost.setEntity(new StringEntity(objectMapper.writeValueAsString(param)));
        return httpPost;
    }

    private Map<String, Object> handleResponse(
            HttpUriRequest request, String responseEntity, String tableName) {
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            CloseableHttpResponse response = httpclient.execute(request);
            final int statusCode = response.getStatusLine().getStatusCode();
            final String reasonPhrase = response.getStatusLine().getReasonPhrase();
            if (statusCode == HTTP_OK && response.getEntity() != null) {
                responseEntity = EntityUtils.toString(response.getEntity());
                LOG.info(responseEntity);
                return objectMapper.readValue(responseEntity, Map.class);
            } else {
                throw new SchemaChangeException(
                        "Failed to create table"
                                + ", table: "
                                + tableName
                                + ", status: "
                                + statusCode
                                + ", reason: "
                                + reasonPhrase);
            }
        } catch (Exception e) {
            LOG.error("SchemaCreation request error,", e);
            throw new SchemaChangeException("SchemaCreation request error with " + e.getMessage());
        }
    }

    private String authHeader() {
        return "Basic "
                + new String(
                        Base64.encodeBase64(
                                (dorisOptions.getUser() + ":" + dorisOptions.getPassword())
                                        .getBytes(StandardCharsets.UTF_8)));
    }
}
