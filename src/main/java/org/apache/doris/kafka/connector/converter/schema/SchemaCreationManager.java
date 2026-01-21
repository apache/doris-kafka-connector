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

import io.debezium.time.MicroTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.kafka.connector.cfg.DorisOptions;
import org.apache.doris.kafka.connector.converter.RecordDescriptor;
import org.apache.doris.kafka.connector.converter.RecordDescriptor.FieldDescriptor;
import org.apache.doris.kafka.connector.converter.type.doris.DorisType;
import org.apache.doris.kafka.connector.exception.SchemaChangeException;
import org.apache.doris.kafka.connector.service.RestService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: Improve table creation configuration properties via doris options
public class SchemaCreationManager {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaCreationManager.class);
    private final DorisOptions dorisOptions;
    private static final String CREATE_DDL = "CREATE TABLE IF NOT EXISTS %s ";

    public SchemaCreationManager(DorisOptions dorisOptions) {
        this.dorisOptions = dorisOptions;
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

    private boolean execute(String tableName, String ddl) {
        if (StringUtils.isEmpty(ddl)) {
            return false;
        }
        LOG.info("Execute SQL: {}", ddl);
        RestService.createTable(dorisOptions, dorisOptions.getDatabase(), tableName, ddl, LOG);
        return true;
    }

    public void createTable(String tableName, RecordDescriptor recordDescriptor) {
        try {
            String statementCreateTableDDL = buildCreateTableDDL(tableName, recordDescriptor);
            boolean status = execute(tableName, statementCreateTableDDL);
            LOG.info(
                    "Created missing {} table from {} database, ddl={}, status={}",
                    tableName,
                    dorisOptions.getDatabase(),
                    statementCreateTableDDL,
                    status);
        } catch (Exception e) {
            LOG.warn("Failed to create table {}, cause by: {}", tableName, e);
            throw new SchemaChangeException(
                    "Failed to create table " + tableName + ", cause by:", e);
        }
    }

    public String buildCreateTableDDL(String tableName, RecordDescriptor recordDescriptor) {

        final StringBuilder dmlBuilder = new StringBuilder();

        dmlBuilder.append(String.format(CREATE_DDL, tableName));
        dmlBuilder.append("(");

        Map<String, FieldDescriptor> allFields = recordDescriptor.getFields();
        Set<String> keyFieldNames = recordDescriptor.getKeyFieldNames();
        List<String> allFieldNames = recordDescriptor.getNonKeyFieldNames();
        List<String> nonKeyFieldNames = new ArrayList<>(allFieldNames);
        Function<String, String> transform =
                (name) -> {
                    final StringBuilder columnSpec = new StringBuilder();
                    final FieldDescriptor field = allFields.get(name);
                    final org.apache.kafka.connect.data.Schema fieldSchema = field.getSchema();
                    final String columnName = field.getName();
                    final String columnType = field.getTypeName();
                    final String[] regKeys = field.getType().getRegistrationKeys();

                    if (regKeys[0].equals(MicroTime.SCHEMA_NAME)) {
                        columnSpec.append(columnName).append(" ").append("VARCHAR(255)");
                    } else if (keyFieldNames.contains(columnName)
                            && columnType.equals(DorisType.STRING)) {
                        columnSpec.append(columnName).append(" ").append("VARCHAR(65533)");
                    } else {
                        columnSpec.append(columnName).append(" ").append(columnType);
                    }
                    columnSpec.append(fieldSchema.isOptional() ? " NULL" : " NOT NULL");

                    return columnSpec.toString();
                };

        nonKeyFieldNames.removeIf(keyFieldNames::contains);

        appendLists(dmlBuilder, keyFieldNames, nonKeyFieldNames, transform);
        dmlBuilder.append(")");

        if (!keyFieldNames.isEmpty()) {
            dmlBuilder.append(
                    String.format(
                            " ENGINE=OLAP UNIQUE KEY (%s) ", String.join(", ", keyFieldNames)));
            dmlBuilder.append(
                    String.format(
                            "DISTRIBUTED BY HASH (%s) BUCKETS AUTO",
                            String.join(", ", keyFieldNames)));
        }

        dmlBuilder.append(";");

        return dmlBuilder.toString();
    }
}
