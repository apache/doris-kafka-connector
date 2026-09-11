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

import static org.apache.doris.kafka.connector.writer.s3.TvfSqlUtils.quoteIdentifier;
import static org.apache.doris.kafka.connector.writer.s3.TvfSqlUtils.quoteLiteral;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.doris.kafka.connector.cfg.DorisOptions;
import org.apache.doris.kafka.connector.connection.ConnectionProvider;
import org.apache.doris.kafka.connector.exception.DorisException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Executes S3 TVF INSERT statements and reconciles ambiguous Label results. */
public class S3TvfLoad {
    private static final Logger LOG = LoggerFactory.getLogger(S3TvfLoad.class);
    private static final int MAX_INSERT_RETRIES = 3;
    private static final int MAX_LABEL_STATE_RETRIES = 3;
    private static final Pattern SESSION_VARIABLE = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private final ConnectionProvider connectionProvider;
    private final S3TvfSqlBuilder sqlBuilder;
    private final String database;
    private final String table;
    private final List<String> columns;
    private final boolean deleteSignEnabled;
    private final Map<String, String> sessionVariables;

    public S3TvfLoad(
            ConnectionProvider connectionProvider,
            DorisOptions options,
            String database,
            String table) {
        this(
                connectionProvider,
                new S3TvfSqlBuilder(options.getS3TvfOptions()),
                database,
                table,
                options.getTvfColumns(),
                options.isEnableDelete(),
                options.getSessionVariables());
    }

    S3TvfLoad(
            ConnectionProvider connectionProvider,
            S3TvfSqlBuilder sqlBuilder,
            String database,
            String table,
            List<String> columns,
            boolean deleteSignEnabled,
            Map<String, String> sessionVariables) {
        this.connectionProvider = connectionProvider;
        this.sqlBuilder = sqlBuilder;
        this.database = database;
        this.table = table;
        this.columns = Collections.unmodifiableList(columns);
        this.deleteSignEnabled = deleteSignEnabled;
        this.sessionVariables = Collections.unmodifiableMap(new LinkedHashMap<>(sessionVariables));
    }

    public void load(String label, List<String> objectKeys) {
        String sql =
                sqlBuilder.buildInsertSql(
                        database, table, label, objectKeys, columns, deleteSignEnabled);
        for (int attempt = 0; attempt <= MAX_INSERT_RETRIES; attempt++) {
            long insertStartedAtNanos = System.nanoTime();
            try {
                executeInsert(sql);
                LOG.info(
                        "S3 TVF insert completed, label={}, objectCount={}, attempt={}, "
                                + "insertTimeMs={}",
                        label,
                        objectKeys.size(),
                        attempt + 1,
                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - insertStartedAtNanos));
                return;
            } catch (SQLException e) {
                LOG.warn(
                        "S3 TVF insert failed, label={}, objectCount={}, attempt={}, "
                                + "insertTimeMs={}, SQLState={}, errorCode={}",
                        label,
                        objectKeys.size(),
                        attempt + 1,
                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - insertStartedAtNanos),
                        e.getSQLState(),
                        e.getErrorCode(),
                        e);
                if (isLabelAlreadyUsed(e, label)) {
                    try {
                        if (handleLabelAlreadyUsed(label)) {
                            return;
                        }
                    } catch (SQLException reconcileFailure) {
                        throw failure(label, reconcileFailure);
                    }
                }
                if (attempt == MAX_INSERT_RETRIES) {
                    throw failure(label, e);
                }
            }
        }
    }

    private void executeInsert(String sql) throws SQLException {
        try (Statement statement = connection().createStatement()) {
            for (Map.Entry<String, String> entry : sessionVariables.entrySet()) {
                if (!SESSION_VARIABLE.matcher(entry.getKey()).matches()) {
                    throw new DorisException("Invalid Doris session variable: " + entry.getKey());
                }
                statement.execute(
                        "SET SESSION " + entry.getKey() + " = " + quoteLiteral(entry.getValue()));
            }
            statement.execute(sql);
        }
    }

    private boolean handleLabelAlreadyUsed(String label) throws SQLException {
        S3TvfLoadState state = getLoadState(label);
        int retries = 0;
        while (true) {
            LOG.info("S3 TVF label {} load state is {}", label, state);
            if (state == S3TvfLoadState.FINISHED) {
                LOG.info("S3 TVF label {} was already committed", label);
                return true;
            }
            if (state == S3TvfLoadState.CANCELLED) {
                LOG.info("S3 TVF label {} was cancelled; retrying the insert", label);
                return false;
            }
            if (state.isActive()) {
                LOG.warn("S3 TVF label {} is {}; cancelling it before retry", label, state);
                try {
                    cancelLoad(label);
                } catch (SQLException e) {
                    // The load may finish between SHOW LOAD and CANCEL LOAD.
                    LOG.warn(
                            "Failed to cancel S3 TVF label {} (SQLState={}, errorCode={}); "
                                    + "rechecking its load state",
                            label,
                            e.getSQLState(),
                            e.getErrorCode());
                }
                state = getLoadState(label);
                if (state == S3TvfLoadState.FINISHED || state == S3TvfLoadState.CANCELLED) {
                    continue;
                }
            }
            if (retries++ >= MAX_LABEL_STATE_RETRIES) {
                break;
            }
            state = getLoadState(label);
        }
        throw new DorisException(
                "Unable to reconcile S3 TVF label " + label + " with state " + state);
    }

    private S3TvfLoadState getLoadState(String label) throws SQLException {
        String sql =
                "SHOW LOAD FROM "
                        + quoteIdentifier(database)
                        + " WHERE LABEL = "
                        + quoteLiteral(label);
        try (Statement statement = connection().createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            S3TvfLoadState resolvedState = S3TvfLoadState.NOT_FOUND;
            while (resultSet.next()) {
                S3TvfLoadState state = parseState(resultSet.getString("State"));
                if (state == S3TvfLoadState.FINISHED) {
                    return state;
                }
                if (state.isActive()
                        || (state == S3TvfLoadState.CANCELLED && !resolvedState.isActive())
                        || resolvedState == S3TvfLoadState.NOT_FOUND) {
                    resolvedState = state;
                }
            }
            return resolvedState;
        }
    }

    private void cancelLoad(String label) throws SQLException {
        String sql =
                "CANCEL LOAD FROM "
                        + quoteIdentifier(database)
                        + " WHERE LABEL = "
                        + quoteLiteral(label);
        try (Statement statement = connection().createStatement()) {
            statement.execute(sql);
        }
    }

    private Connection connection() throws SQLException {
        try {
            return connectionProvider.getOrEstablishConnection();
        } catch (Exception e) {
            throw new SQLException("Unable to establish Doris JDBC connection", e);
        }
    }

    private static S3TvfLoadState parseState(String value) {
        if (value == null) {
            return S3TvfLoadState.UNKNOWN;
        }
        try {
            return S3TvfLoadState.valueOf(value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return S3TvfLoadState.UNKNOWN;
        }
    }

    private static boolean isLabelAlreadyUsed(Throwable throwable, String label) {
        String marker = "Label [" + label + "] has already been used";
        Throwable current = throwable;
        while (current != null) {
            if (current.getMessage() != null && current.getMessage().contains(marker)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static DorisException failure(String label, Throwable cause) {
        return new DorisException("Failed to commit S3 TVF label " + label, cause);
    }
}
