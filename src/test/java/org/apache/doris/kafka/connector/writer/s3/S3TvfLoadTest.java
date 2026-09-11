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

package org.apache.doris.kafka.connector.writer.s3;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.StringWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.doris.kafka.connector.cfg.S3TvfOptions;
import org.apache.doris.kafka.connector.connection.ConnectionProvider;
import org.apache.doris.kafka.connector.exception.DorisException;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.WriterAppender;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

public class S3TvfLoadTest {
    private ConnectionProvider connectionProvider;
    private Connection connection;
    private Statement statement;
    private String insertSql;
    private StringWriter logs;
    private WriterAppender appender;

    @Before
    public void setUp() throws Exception {
        connectionProvider = mock(ConnectionProvider.class);
        connection = mock(Connection.class);
        statement = mock(Statement.class);
        when(connectionProvider.getOrEstablishConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        insertSql =
                sqlBuilder().buildInsertSql("demo", "orders", "label", files(), columns(), false);
        logs = new StringWriter();
        appender = new WriterAppender(new PatternLayout("%m%n"), logs);
        org.apache.log4j.Logger.getLogger(S3TvfLoad.class).addAppender(appender);
    }

    @After
    public void tearDown() {
        org.apache.log4j.Logger.getLogger(S3TvfLoad.class).removeAppender(appender);
        appender.close();
    }

    @Test
    public void testLogsInsertMetrics() {
        load(Collections.emptyMap()).load("label", files());

        String output = logs.toString();
        Assert.assertTrue(
                output.contains("S3 TVF insert completed, label=label, objectCount=1, attempt=1"));
        Assert.assertTrue(output.matches("(?s).*insertTimeMs=\\d+.*"));
    }

    @Test
    public void testLogsInsertFailureCause() throws Exception {
        when(statement.execute(insertSql)).thenThrow(new SQLException("temporary"));

        try {
            load(Collections.emptyMap()).load("label", files());
            Assert.fail("Expected load failure");
        } catch (DorisException expected) {
            // The warning must keep the SQLException stack trace for diagnosis.
        }

        Assert.assertTrue(logs.toString().contains("java.sql.SQLException: temporary"));
    }

    @Test
    public void testSetsSessionVariablesBeforeInsert() throws Exception {
        Map<String, String> sessionVariables = new LinkedHashMap<>();
        sessionVariables.put("enable_unique_key_partial_update", "true");
        S3TvfLoad load = load(sessionVariables);

        load.load("label", files());

        InOrder order = inOrder(statement);
        order.verify(statement).execute("SET SESSION enable_unique_key_partial_update = 'true'");
        order.verify(statement).execute(insertSql);
    }

    @Test
    public void testRetriesTransientInsertFailure() throws Exception {
        when(statement.execute(insertSql))
                .thenThrow(new SQLException("temporary"))
                .thenReturn(true);

        load(Collections.emptyMap()).load("label", files());

        verify(statement, org.mockito.Mockito.times(2)).execute(insertSql);
        verify(statement, never()).executeQuery(anyString());
    }

    @Test
    public void testLabelAlreadyUsedFinishedIsSuccess() throws Exception {
        when(statement.execute(insertSql))
                .thenThrow(new SQLException("Label [label] has already been used"));
        doReturn(resultSet("FINISHED"))
                .when(statement)
                .executeQuery("SHOW LOAD FROM `demo` WHERE LABEL = 'label'");

        load(Collections.emptyMap()).load("label", files());

        verify(statement).executeQuery("SHOW LOAD FROM `demo` WHERE LABEL = 'label'");
    }

    @Test
    public void testFinishedWinsOverCancelledRows() throws Exception {
        when(statement.execute(insertSql))
                .thenThrow(new SQLException("Label [label] has already been used"));
        doReturn(resultSet("CANCELLED", "FINISHED"))
                .when(statement)
                .executeQuery("SHOW LOAD FROM `demo` WHERE LABEL = 'label'");

        load(Collections.emptyMap()).load("label", files());
    }

    @Test
    public void testCancelsActiveLabelAndAcceptsConcurrentFinish() throws Exception {
        when(statement.execute(insertSql))
                .thenThrow(new SQLException("Label [label] has already been used"));
        doReturn(resultSet("LOADING"), resultSet("FINISHED"))
                .when(statement)
                .executeQuery("SHOW LOAD FROM `demo` WHERE LABEL = 'label'");

        load(Collections.emptyMap()).load("label", files());

        verify(statement).execute("CANCEL LOAD FROM `demo` WHERE LABEL = 'label'");
        verify(statement, org.mockito.Mockito.times(2))
                .executeQuery("SHOW LOAD FROM `demo` WHERE LABEL = 'label'");
    }

    @Test
    public void testCancelledLabelRetriesInsert() throws Exception {
        when(statement.execute(insertSql))
                .thenThrow(new SQLException("Label [label] has already been used"))
                .thenReturn(true);
        doReturn(resultSet("CANCELLED"))
                .when(statement)
                .executeQuery("SHOW LOAD FROM `demo` WHERE LABEL = 'label'");

        load(Collections.emptyMap()).load("label", files());

        verify(statement, org.mockito.Mockito.times(2)).execute(insertSql);
    }

    @Test(expected = DorisException.class)
    public void testRejectsInvalidSessionVariableName() throws Exception {
        load(Collections.singletonMap("invalid-name", "true")).load("label", files());
    }

    @Test
    public void testFailureDoesNotExposeSqlOrCredentials() throws Exception {
        when(statement.execute(insertSql)).thenThrow(new SQLException("temporary"));
        try {
            load(Collections.emptyMap()).load("label", files());
            Assert.fail("Expected load failure");
        } catch (DorisException e) {
            Assert.assertFalse(e.getMessage().contains("secret-key"));
            Assert.assertFalse(e.getMessage().contains("INSERT INTO"));
            Assert.assertTrue(e.getMessage().contains("label"));
        }
        verify(statement, org.mockito.Mockito.times(4)).execute(insertSql);
    }

    private S3TvfLoad load(Map<String, String> sessionVariables) {
        return new S3TvfLoad(
                connectionProvider,
                sqlBuilder(),
                "demo",
                "orders",
                columns(),
                false,
                sessionVariables);
    }

    private static S3TvfSqlBuilder sqlBuilder() {
        S3TvfOptions options =
                S3TvfOptions.builder()
                        .setEndpoint("https://s3.example.com")
                        .setRegion("us-east-1")
                        .setBucket("staging")
                        .setPrefix("objects")
                        .setAccessKey("access-key")
                        .setSecretKey("secret-key")
                        .build();
        return new S3TvfSqlBuilder(options);
    }

    private static java.util.List<String> files() {
        return Arrays.asList("objects/file.json");
    }

    private static java.util.List<String> columns() {
        return Arrays.asList("id", "name");
    }

    private static ResultSet resultSet(String... states) throws SQLException {
        ResultSet resultSet = mock(ResultSet.class);
        org.mockito.stubbing.OngoingStubbing<Boolean> next = when(resultSet.next());
        for (int index = 0; index < states.length; index++) {
            next = next.thenReturn(true);
        }
        next.thenReturn(false);

        org.mockito.stubbing.OngoingStubbing<String> state = when(resultSet.getString("State"));
        for (String value : states) {
            state = state.thenReturn(value);
        }
        return resultSet;
    }
}
