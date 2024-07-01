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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.doris.kafka.connector.e2e.doris.DorisContainerService;
import org.apache.doris.kafka.connector.e2e.doris.DorisContainerServiceImpl;
import org.apache.doris.kafka.connector.e2e.kafka.KafkaContainerService;
import org.apache.doris.kafka.connector.e2e.kafka.KafkaContainerServiceImpl;
import org.apache.doris.kafka.connector.exception.DorisException;
import org.junit.AfterClass;
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

    protected static Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                String.format(JDBC_URL, dorisInstanceHost), USERNAME, PASSWORD);
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

    protected static void createDatabase(String databaseName) {
        LOG.info("Will to be create database, sql={}", databaseName);
        try {
            Statement statement = getJdbcConnection().createStatement();
            statement.execute("create database if not exists " + databaseName);
        } catch (SQLException e) {
            throw new DorisException("Failed to create doris table.", e);
        }
        LOG.info("Create database successfully. databaseName={}", databaseName);
    }

    protected void createTable(String sql) {
        LOG.info("Will to be create doris table, sql={}", sql);
        try {
            Statement statement = getJdbcConnection().createStatement();
            statement.execute(sql);
        } catch (SQLException e) {
            throw new DorisException("Failed to create doris table.", e);
        }
        LOG.info("Create doris table successfully. sql={}", sql);
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
        kafkaContainerService.close();
        dorisContainerService.close();
    }
}
