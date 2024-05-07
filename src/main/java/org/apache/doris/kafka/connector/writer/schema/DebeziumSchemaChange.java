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

package org.apache.doris.kafka.connector.writer.schema;

import io.debezium.data.Envelope;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.doris.kafka.connector.cfg.DorisOptions;
import org.apache.doris.kafka.connector.connection.ConnectionProvider;
import org.apache.doris.kafka.connector.converter.RecordDescriptor;
import org.apache.doris.kafka.connector.exception.SchemaChangeException;
import org.apache.doris.kafka.connector.metrics.DorisConnectMonitor;
import org.apache.doris.kafka.connector.model.ColumnDescriptor;
import org.apache.doris.kafka.connector.model.TableDescriptor;
import org.apache.doris.kafka.connector.model.doris.Schema;
import org.apache.doris.kafka.connector.service.DorisSystemService;
import org.apache.doris.kafka.connector.service.RestService;
import org.apache.doris.kafka.connector.writer.DorisWriter;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebeziumSchemaChange extends DorisWriter {
    private static final Logger LOG = LoggerFactory.getLogger(DebeziumSchemaChange.class);
    private final Set<String> sinkTableSet;
    private final DorisSystemService dorisSystemService;
    private final Map<String, String> topic2TableMap;
    private final SchemaChangeManager schemaChangeManager;

    public DebeziumSchemaChange(
            String topic,
            int partition,
            DorisOptions dorisOptions,
            ConnectionProvider connectionProvider,
            DorisConnectMonitor connectMonitor) {
        super(topic, partition, dorisOptions, connectionProvider, connectMonitor);
        this.schemaChange = true;
        this.sinkTableSet = new HashSet<>();
        this.dorisSystemService = new DorisSystemService(dorisOptions);
        this.topic2TableMap = dorisOptions.getTopicMap();
        this.schemaChangeManager = new SchemaChangeManager(dorisOptions);
        init();
    }

    @Override
    public void fetchOffset() {
        // do nothing
    }

    private void init() {
        Set<Map.Entry<String, String>> entrySet = topic2TableMap.entrySet();
        for (Map.Entry<String, String> entry : entrySet) {
            sinkTableSet.add(entry.getValue());
        }
    }

    @Override
    public void insert(SinkRecord record) {
        schemaChange(record);
    }

    @Override
    public void commit(int partition) {
        // do nothing
    }

    private void schemaChange(final SinkRecord record) {
        String tableName = resolveTableName(record);
        if (tableName == null) {
            LOG.warn(
                    "Ignored to write record from topic '{}' partition '{}' offset '{}'. No resolvable table name",
                    record.topic(),
                    record.kafkaPartition(),
                    record.kafkaOffset());
            processedOffset.set(record.kafkaOffset());
            return;
        }
        Struct recordStruct = (Struct) (record.value());
        List<Object> tableChanges = recordStruct.getArray("tableChanges");
        Struct tableChange = (Struct) tableChanges.get(0);
        if ("DROP".equalsIgnoreCase(tableChange.getString("type"))
                || "CREATE".equalsIgnoreCase(tableChange.getString("type"))) {
            LOG.warn(
                    "CREATE and DROP {} tables are currently not supported. Please create or drop them manually.",
                    tableName);
            processedOffset.set(record.kafkaOffset());
            return;
        }
        RecordDescriptor recordDescriptor =
                RecordDescriptor.builder()
                        .withSinkRecord(record)
                        .withTableChange(tableChange)
                        .build();
        tableChange(tableName, recordDescriptor);
    }

    private String resolveTableName(SinkRecord record) {
        if (isTombstone(record)) {
            LOG.warn(
                    "Ignore this record because it seems to be a tombstone that doesn't have source field, then cannot resolve table name in topic '{}', partition '{}', offset '{}'",
                    record.topic(),
                    record.kafkaPartition(),
                    record.kafkaOffset());
            return null;
        }
        Struct source = ((Struct) record.value()).getStruct(Envelope.FieldName.SOURCE);
        return source.getString("table");
    }

    private void alterTableIfNeeded(String tableName, RecordDescriptor record) {
        LOG.debug("Attempting to alter table '{}'.", tableName);
        if (!hasTable(tableName)) {
            LOG.error("Table '{}' does not exist and cannot be altered.", tableName);
            throw new SchemaChangeException("Could not find table: " + tableName);
        }
        final TableDescriptor dorisTableDescriptor = obtainTableSchema(tableName);
        SchemaChangeHelper.compareSchema(dorisTableDescriptor, record.getFields());
        List<String> ddlSql =
                SchemaChangeHelper.generateDDLSql(dorisOptions.getDatabase(), tableName);
        doSchemaChange(dorisOptions.getDatabase(), tableName, ddlSql);
    }

    /** Obtain table schema from doris. */
    private TableDescriptor obtainTableSchema(String tableName) {
        Schema schema = RestService.getSchema(dorisOptions, dbName, tableName, LOG);
        List<ColumnDescriptor> columnDescriptors = new ArrayList<>();
        schema.getProperties()
                .forEach(
                        column -> {
                            ColumnDescriptor columnDescriptor =
                                    ColumnDescriptor.builder()
                                            .columnName(column.getName())
                                            .typeName(column.getType())
                                            .comment(column.getComment())
                                            .build();
                            columnDescriptors.add(columnDescriptor);
                        });
        return TableDescriptor.builder()
                .tableName(tableName)
                .type(schema.getKeysType())
                .columns(columnDescriptors)
                .build();
    }

    private boolean hasTable(String tableName) {
        return dorisSystemService.tableExists(dbName, tableName);
    }

    private void tableChange(String tableName, RecordDescriptor recordDescriptor) {
        if (!sinkTableSet.contains(tableName)) {
            processedOffset.set(recordDescriptor.getOffset());
            LOG.warn(
                    "The "
                            + tableName
                            + " is not defined and requires synchronized data. If you need to synchronize the table data, please configure it in 'doris.topic2table.map'");
            return;
        }

        if (!hasTable(tableName)) {
            // TODO Table does not exist, automatically created it.
            LOG.error("{} Table does not exist, please create manually.", tableName);
        } else {
            // Table exists, lets attempt to alter it if necessary.
            alterTableIfNeeded(tableName, recordDescriptor);
        }
        processedOffset.set(recordDescriptor.getOffset());
    }

    private boolean doSchemaChange(String database, String tableName, List<String> ddlList) {
        boolean status = false;
        if (ddlList.isEmpty()) {
            LOG.info("Schema change ddl is empty, not need do schema change.");
            return false;
        }
        try {
            List<SchemaChangeHelper.DDLSchema> ddlSchemas = SchemaChangeHelper.getDdlSchemas();
            for (int i = 0; i < ddlList.size(); i++) {
                SchemaChangeHelper.DDLSchema ddlSchema = ddlSchemas.get(i);
                String ddlSql = ddlList.get(i);
                boolean doSchemaChange = checkSchemaChange(database, tableName, ddlSchema);
                status =
                        doSchemaChange
                                && schemaChangeManager.execute(ddlSql, dorisOptions.getDatabase());
                LOG.info("schema change status:{}, ddl:{}", status, ddlSql);
            }
        } catch (Exception e) {
            LOG.warn("schema change error :", e);
        }
        return status;
    }

    private boolean checkSchemaChange(
            String database, String table, SchemaChangeHelper.DDLSchema ddlSchema)
            throws IllegalArgumentException, IOException {
        Map<String, Object> param =
                SchemaChangeManager.buildRequestParam(
                        ddlSchema.isDropColumn(), ddlSchema.getColumnName());
        return schemaChangeManager.checkSchemaChange(database, table, param);
    }

    public long getOffset() {
        committedOffset.set(processedOffset.get());
        return committedOffset.get();
    }

    private boolean isTombstone(SinkRecord record) {
        return record.value() == null;
    }
}
