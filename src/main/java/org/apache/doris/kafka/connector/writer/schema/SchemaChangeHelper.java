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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.kafka.connector.converter.RecordDescriptor;
import org.apache.doris.kafka.connector.model.ColumnDescriptor;
import org.apache.doris.kafka.connector.model.TableDescriptor;

public class SchemaChangeHelper {
    private static final List<String> dropFieldSchemas = Lists.newArrayList();
    private static final List<ColumnDescriptor> addFieldSchemas = Lists.newArrayList();
    // Used to determine whether the doris table supports ddl
    private static final List<DDLSchema> ddlSchemas = Lists.newArrayList();
    private static final String ADD_DDL = "ALTER TABLE %s ADD COLUMN %s %s";
    private static final String DROP_DDL = "ALTER TABLE %s DROP COLUMN %s";
    // TODO support rename column
    private static final String RENAME_DDL = "ALTER TABLE %s RENAME COLUMN %s %s";

    /**
     * Compare kafka upstream table structure with doris table structure. If kafka field does not
     * contain the structure of dorisTable, then need to add this field, if dorisTable does not
     * contain fields in kafka fields, then need to delete them.
     *
     * @param dorisTable read from the table schema of doris.
     * @param fields table structure from kafka upstream data source.
     */
    public static void compareSchema(
            TableDescriptor dorisTable, Map<String, RecordDescriptor.FieldDescriptor> fields) {
        dropFieldSchemas.clear();
        addFieldSchemas.clear();
        // Determine whether fields need to be dropped in doris table
        Collection<ColumnDescriptor> dorisTableColumns = dorisTable.getColumns();
        for (ColumnDescriptor dorisColumn : dorisTableColumns) {
            if (!fields.containsKey(dorisColumn.getColumnName())) {
                dropFieldSchemas.add(dorisColumn.getColumnName());
            }
        }
        // Determine whether fields need to be added to doris table
        Set<String> dorisTableColumnNames =
                dorisTableColumns.stream()
                        .map(ColumnDescriptor::getColumnName)
                        .collect(Collectors.toSet());
        Set<Map.Entry<String, RecordDescriptor.FieldDescriptor>> fieldsEntries = fields.entrySet();
        for (Map.Entry<String, RecordDescriptor.FieldDescriptor> fieldEntry : fieldsEntries) {
            String fieldName = fieldEntry.getKey();
            if (!dorisTableColumnNames.contains(fieldName)) {
                RecordDescriptor.FieldDescriptor fieldDescriptor = fieldEntry.getValue();
                ColumnDescriptor columnDescriptor =
                        new ColumnDescriptor.Builder()
                                .columnName(fieldDescriptor.getName())
                                .typeName(fieldDescriptor.getSchemaTypeName())
                                .defaultValue(fieldDescriptor.getDefaultValue())
                                .comment(fieldDescriptor.getComment())
                                .build();
                addFieldSchemas.add(columnDescriptor);
            }
        }
    }

    public static List<String> generateDDLSql(String database, String table) {
        ddlSchemas.clear();
        List<String> ddlList = Lists.newArrayList();
        for (ColumnDescriptor columnDescriptor : addFieldSchemas) {
            ddlList.add(buildAddColumnDDL(database, table, columnDescriptor));
            ddlSchemas.add(new DDLSchema(columnDescriptor.getColumnName(), false));
        }
        for (String columName : dropFieldSchemas) {
            ddlList.add(buildDropColumnDDL(database, table, columName));
            ddlSchemas.add(new DDLSchema(columName, true));
        }
        return ddlList;
    }

    public static List<DDLSchema> getDdlSchemas() {
        return ddlSchemas;
    }

    private static String buildDropColumnDDL(String database, String tableName, String columName) {
        return String.format(
                DROP_DDL,
                identifier(database) + "." + identifier(tableName),
                identifier(columName));
    }

    private static String buildAddColumnDDL(
            String database, String tableName, ColumnDescriptor columnDescriptor) {
        String columnName = columnDescriptor.getColumnName();
        String columnType = columnDescriptor.getTypeName();
        String defaultValue = columnDescriptor.getDefaultValue();
        String comment = columnDescriptor.getComment();
        String addDDL =
                String.format(
                        ADD_DDL,
                        identifier(database) + "." + identifier(tableName),
                        identifier(columnName),
                        columnType);
        if (defaultValue != null) {
            addDDL = addDDL + " DEFAULT " + quoteDefaultValue(defaultValue);
        }
        if (StringUtils.isNotEmpty(comment)) {
            addDDL = addDDL + " COMMENT '" + quoteComment(comment) + "'";
        }
        return addDDL;
    }

    private static String identifier(String name) {
        return "`" + name + "`";
    }

    private static String quoteDefaultValue(String defaultValue) {
        // DEFAULT current_timestamp not need quote
        if (defaultValue.equalsIgnoreCase("current_timestamp")) {
            return defaultValue;
        }
        return "'" + defaultValue + "'";
    }

    private static String quoteComment(String comment) {
        return comment.replaceAll("'", "\\\\'");
    }

    public static class DDLSchema {
        private final String columnName;
        private final boolean isDropColumn;

        public DDLSchema(String columnName, boolean isDropColumn) {
            this.columnName = columnName;
            this.isDropColumn = isDropColumn;
        }

        public String getColumnName() {
            return columnName;
        }

        public boolean isDropColumn() {
            return isDropColumn;
        }
    }
}
