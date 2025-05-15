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

package org.apache.doris.kafka.connector.e2e.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.kafka.connector.e2e.doris.DorisContainerService;
import org.apache.doris.kafka.connector.e2e.doris.DorisContainerServiceImpl;
import org.apache.doris.kafka.connector.e2e.kafka.KafkaContainerService;
import org.apache.doris.kafka.connector.e2e.kafka.KafkaContainerServiceImpl;
import org.apache.doris.kafka.connector.exception.DorisException;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractKafka2DorisSink {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractKafka2DorisSink.class);
    protected static final String NAME = "name";
    protected static final String CONFIG = "config";
    private static final String JDBC_URL = "jdbc:mysql://%s:9030";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "";
    private static String dorisInstanceHost;
    protected static String kafkaInstanceHostAndPort;
    protected static KafkaContainerService kafkaContainerService;
    protected static ObjectMapper objectMapper = new ObjectMapper();
    private static DorisContainerService dorisContainerService;

    @BeforeClass
    public static void initServer() {
        initDorisBase();
        initKafka();
    }

    private static void initKafka() {
        if (Objects.nonNull(kafkaContainerService)) {
            return;
        }
        kafkaContainerService = new KafkaContainerServiceImpl();
        kafkaContainerService.startContainer();
        kafkaContainerService.startConnector();
        kafkaInstanceHostAndPort = kafkaContainerService.getInstanceHostAndPort();
    }

    protected static void initSchemaRegistry() {
        if (Objects.isNull(kafkaContainerService)) {
            return;
        }
        kafkaContainerService.startSchemaRegistry();
    }

    protected static String getSchemaRegistryUrl() {
        return kafkaContainerService.getSchemaRegistryUrl();
    }

    protected static Connection getJdbcConnection() {
        try {
            return DriverManager.getConnection(
                    String.format(JDBC_URL, dorisInstanceHost), USERNAME, PASSWORD);
        } catch (SQLException e) {
            throw new DorisException(e);
        }
    }

    protected static String loadContent(String path) {
        try (InputStream stream = Files.newInputStream(Paths.get(path))) {
            return new BufferedReader(new InputStreamReader(Objects.requireNonNull(stream)))
                    .lines()
                    .collect(Collectors.joining("\n"));
        } catch (IOException e) {
            throw new DorisException("Failed to read " + path + " file.", e);
        }
    }

    protected static void executeSql(Connection connection, String... sql) {
        if (sql == null || sql.length == 0) {
            return;
        }
        try (Statement statement = connection.createStatement()) {
            for (String s : sql) {
                if (StringUtils.isNotEmpty(s)) {
                    statement.execute(s);
                }
            }
        } catch (SQLException e) {
            LOG.error("Failed to execute sql.", e);
            throw new DorisException(e);
        }
    }

    protected static void createDatabase(String databaseName) {
        LOG.info("Will to be create database, sql={}", databaseName);
        try (Connection conn = getJdbcConnection();
                Statement statement = conn.createStatement()) {
            statement.execute("create database if not exists " + databaseName);
        } catch (SQLException e) {
            throw new DorisException("Failed to create doris table.", e);
        }
        LOG.info("Create database successfully. databaseName={}", databaseName);
    }

    protected void createTable(String sql) {
        LOG.info("Will to be create doris table, sql={}", sql);
        try (Connection conn = getJdbcConnection();
                Statement statement = conn.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new DorisException("Failed to create doris table.", e);
        }
        LOG.info("Create doris table successfully. sql={}", sql);
    }

    protected void insertTable(String sql) {
        LOG.info("Will insert data to Doris table. SQL: {}", sql);
        try (Connection conn = getJdbcConnection();
                Statement statement = conn.createStatement()) {
            int rowCount = statement.executeUpdate(sql);
            LOG.info("Inserted {} item data into the Doris table.", rowCount);
        } catch (SQLException e) {
            throw new DorisException("Failed to insert data to Doris table.", e);
        }
        LOG.info("Data insertion to Doris table was successful. SQL: {}", sql);
    }

    private static void initDorisBase() {
        if (Objects.nonNull(dorisContainerService)) {
            return;
        }
        dorisContainerService = new DorisContainerServiceImpl();
        dorisContainerService.startContainer();
        dorisInstanceHost = dorisContainerService.getInstanceHost();
    }

    @AfterClass
    public static void close() {
        // Closed automatically, multiple itcases can be reused
        // kafkaContainerService.close();
        // dorisContainerService.close();
    }

    public void checkResult(List<String> expected, String query, int columnSize) throws Exception {
        List<String> actual = new ArrayList<>();

        try (Connection conn = getJdbcConnection();
                Statement statement = conn.createStatement()) {
            ResultSet sinkResultSet = statement.executeQuery(query);
            while (sinkResultSet.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= columnSize; i++) {
                    Object value = sinkResultSet.getObject(i);
                    if (value == null) {
                        row.add("null");
                    } else {
                        row.add(value.toString());
                    }
                }
                actual.add(StringUtils.join(row, ","));
            }
        }
        LOG.info("expected result: {}", Arrays.toString(expected.toArray()));
        LOG.info("actual result: {}", Arrays.toString(actual.toArray()));
        Assert.assertArrayEquals(expected.toArray(), actual.toArray());
    }

    protected void faultInjectionOpen() throws IOException {
        String pointName = "FlushToken.submit_flush_error";
        String apiUrl =
                String.format(
                        "http://%s:%s/api/debug_point/add/%s",
                        dorisContainerService.getInstanceHost(), 8040, pointName);
        HttpPost httpPost = new HttpPost(apiUrl);
        httpPost.addHeader(HttpHeaders.AUTHORIZATION, auth(USERNAME, PASSWORD));
        try (CloseableHttpClient httpClient = HttpClients.custom().build()) {
            try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
                int statusCode = response.getStatusLine().getStatusCode();
                String reason = response.getStatusLine().toString();
                if (statusCode == 200 && response.getEntity() != null) {
                    LOG.info("Debug point response {}", EntityUtils.toString(response.getEntity()));
                } else {
                    LOG.info("Debug point failed, statusCode: {}, reason: {}", statusCode, reason);
                }
            }
        }
    }

    protected void faultInjectionClear() throws IOException {
        String apiUrl =
                String.format(
                        "http://%s:%s/api/debug_point/clear",
                        dorisContainerService.getInstanceHost(), 8040);
        HttpPost httpPost = new HttpPost(apiUrl);
        httpPost.addHeader(HttpHeaders.AUTHORIZATION, auth(USERNAME, PASSWORD));
        try (CloseableHttpClient httpClient = HttpClients.custom().build()) {
            try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
                int statusCode = response.getStatusLine().getStatusCode();
                String reason = response.getStatusLine().toString();
                if (statusCode == 200 && response.getEntity() != null) {
                    LOG.info("Debug point response {}", EntityUtils.toString(response.getEntity()));
                } else {
                    LOG.info("Debug point failed, statusCode: {}, reason: {}", statusCode, reason);
                }
            }
        }
    }

    protected String auth(String user, String password) {
        final String authInfo = user + ":" + password;
        byte[] encoded = Base64.encodeBase64(authInfo.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }
}
