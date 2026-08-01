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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.doris.kafka.connector.cfg.DorisOptions;
import org.apache.doris.kafka.connector.cfg.DorisSinkConnectorConfig;
import org.apache.doris.kafka.connector.converter.RecordDescriptor;
import org.apache.doris.kafka.connector.converter.RecordTypeRegister;
import org.apache.doris.kafka.connector.writer.TestRecordBuffer;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestSchemaCreationManager {

    private JsonConverter jsonConverter = new JsonConverter();
    private SchemaCreationManager schemaCreationManager;
    private RecordTypeRegister recordTypeRegister;
    private Properties props = new Properties();
    private String database = "default_cluster";
    // private MockedStatic<RestService> mockRestService;

    @Before
    public void init() throws IOException {
        try (InputStream stream =
                this.getClass()
                        .getClassLoader()
                        .getResourceAsStream("doris-connector-sink.properties")) {
            Assert.assertNotNull("doris-connector-sink.properties not found on classpath", stream);
            props.load(stream);
        }
        DorisSinkConnectorConfig.setDefaultValues((Map) props);
        props.put("doris.database", "default_cluster");
        props.put("task_id", "1");
        props.put("converter.mode", "debezium_ingestion");
        props.put("debezium.schema.evolution", "basic");
        props.put(
                "doris.topic2table.map",
                "debezium_postgresql.wdl_test.psql_common_table:psql_common_table,debezium_postgresql.wdl_test.psql_composite_table:psql_composite_table,debezium_mysql.wdl_test.mysql_common_table:mysql_common_table,debezium_mysql.wdl_test.mysql_geo_table:mysql_geo_table");

        schemaCreationManager = new SchemaCreationManager(new DorisOptions((Map) props));
        recordTypeRegister = new RecordTypeRegister(new DorisOptions((Map) props));
        HashMap<String, String> config = new HashMap<>();
        jsonConverter.configure(config, false);
        // mockRestService = mockStatic(RestService.class);
    }

    @After
    public void close() {
        // mockRestService.close();
    }

    @Test
    public void buildCreateTableDDLWithPsqlDbzCompositeKeyStructRecord() throws IOException {
        // common
        String psqlDbzTopic = "debezium_postgresql.wdl_test.psql_composite_table";
        String psqlDbzTable = "psql_composite_table";
        // key
        String psqlDbzStructKeyString =
                "{\"schema\": {\"type\": \"struct\", \"fields\": [{\"type\": \"int32\", \"optional\": false, \"field\": \"student_id\"}, {\"type\": \"string\", \"optional\": false, \"field\": \"course_code\"}], \"optional\": false, \"name\": \"debezium_postgresql.wdl_test.psql_composite_table.Key\"}, \"payload\": {\"student_id\": 1, \"course_code\": \"CS101\"}}";
        // value
        String psqlDbzStructNonKeyString =
                "{\"schema\": {\"type\": \"struct\", \"fields\": [{\"type\": \"struct\", \"fields\": [{\"type\": \"int32\", \"optional\": false, \"field\": \"student_id\"}, {\"type\": \"string\", \"optional\": false, \"field\": \"course_code\"}, {\"type\": \"int32\", \"optional\": true, \"field\": \"grade\"}], \"optional\": true, \"name\": \"debezium_postgresql.wdl_test.psql_composite_table.Value\", \"field\": \"before\"}, {\"type\": \"struct\", \"fields\": [{\"type\": \"int32\", \"optional\": false, \"field\": \"student_id\"}, {\"type\": \"string\", \"optional\": false, \"field\": \"course_code\"}, {\"type\": \"int32\", \"optional\": true, \"field\": \"grade\"}], \"optional\": true, \"name\": \"debezium_postgresql.wdl_test.psql_composite_table.Value\", \"field\": \"after\"}, {\"type\": \"struct\", \"fields\": [{\"type\": \"string\", \"optional\": false, \"field\": \"version\"}, {\"type\": \"string\", \"optional\": false, \"field\": \"connector\"}, {\"type\": \"string\", \"optional\": false, \"field\": \"name\"}, {\"type\": \"int64\", \"optional\": false, \"field\": \"ts_ms\"}, {\"type\": \"string\", \"optional\": true, \"name\": \"io.debezium.data.Enum\", \"version\": 1, \"parameters\": {\"allowed\": \"true,first,first_in_data_collection,last_in_data_collection,last,false,incremental\"}, \"default\": \"false\", \"field\": \"snapshot\"}, {\"type\": \"string\", \"optional\": false, \"field\": \"db\"}, {\"type\": \"string\", \"optional\": true, \"field\": \"sequence\"}, {\"type\": \"int64\", \"optional\": true, \"field\": \"ts_us\"}, {\"type\": \"int64\", \"optional\": true, \"field\": \"ts_ns\"}, {\"type\": \"string\", \"optional\": false, \"field\": \"schema\"}, {\"type\": \"string\", \"optional\": false, \"field\": \"table\"}, {\"type\": \"int64\", \"optional\": true, \"field\": \"txId\"}, {\"type\": \"int64\", \"optional\": true, \"field\": \"lsn\"}, {\"type\": \"int64\", \"optional\": true, \"field\": \"xmin\"}], \"optional\": false, \"name\": \"io.debezium.connector.postgresql.Source\", \"version\": 1, \"field\": \"source\"}, {\"type\": \"struct\", \"fields\": [{\"type\": \"string\", \"optional\": false, \"field\": \"id\"}, {\"type\": \"int64\", \"optional\": false, \"field\": \"total_order\"}, {\"type\": \"int64\", \"optional\": false, \"field\": \"data_collection_order\"}], \"optional\": true, \"name\": \"event.block\", \"version\": 1, \"field\": \"transaction\"}, {\"type\": \"string\", \"optional\": false, \"field\": \"op\"}, {\"type\": \"int64\", \"optional\": true, \"field\": \"ts_ms\"}, {\"type\": \"int64\", \"optional\": true, \"field\": \"ts_us\"}, {\"type\": \"int64\", \"optional\": true, \"field\": \"ts_ns\"}], \"optional\": false, \"name\": \"debezium_postgresql.wdl_test.psql_composite_table.Envelope\", \"version\": 2}, \"payload\": {\"before\": null, \"after\": {\"student_id\": 1, \"course_code\": \"CS101\", \"grade\": 95}, \"source\": {\"version\": \"3.3.1.Final\", \"connector\": \"postgresql\", \"name\": \"wsl_dbz\", \"ts_ms\": 1767628768669, \"snapshot\": \"first_in_data_collection\", \"db\": \"sink\", \"sequence\": \"[null,\\\"75101088\\\"]\", \"ts_us\": 1767628768669672, \"ts_ns\": 1767628768669672000, \"schema\": \"public\", \"table\": \"course_grades\", \"txId\": 1032, \"lsn\": 75101088, \"xmin\": null}, \"transaction\": null, \"op\": \"r\", \"ts_ms\": 1767628768901, \"ts_us\": 1767628768901563, \"ts_ns\": 1767628768901563641}}";

        SchemaAndValue psqlDbzSchemaValueNonKey =
                jsonConverter.toConnectData(
                        psqlDbzTopic, psqlDbzStructNonKeyString.getBytes(StandardCharsets.UTF_8));

        SchemaAndValue psqlDbzSchemaValueKey =
                jsonConverter.toConnectData(
                        psqlDbzTopic, psqlDbzStructKeyString.getBytes(StandardCharsets.UTF_8));

        SinkRecord psqlDbzRecord =
                TestRecordBuffer.newSinkRecord(
                        psqlDbzTopic,
                        psqlDbzSchemaValueKey.schema(),
                        psqlDbzSchemaValueNonKey.schema(),
                        psqlDbzSchemaValueKey,
                        psqlDbzSchemaValueNonKey,
                        0);

        RecordDescriptor recordDescriptor =
                RecordDescriptor.builder()
                        .withSinkRecord(psqlDbzRecord)
                        .withTypeRegistry(recordTypeRegister.getTypeRegistry())
                        .build();

        String expectedDDL =
                "CREATE TABLE IF NOT EXISTS `default_cluster`.`psql_composite_table` (`student_id` INT NOT NULL, `course_code` VARCHAR(65533) NOT NULL, `grade` INT NULL) ENGINE=OLAP UNIQUE KEY (`student_id`, `course_code`) DISTRIBUTED BY HASH (`student_id`, `course_code`) BUCKETS AUTO PROPERTIES (\"replication_num\" = \"3\");";

        String ddlCreateTable =
                schemaCreationManager.buildCreateTableDDL(database, psqlDbzTable, recordDescriptor);

        Assert.assertEquals(expectedDDL, ddlCreateTable);
    }

    @Test
    public void buildCreateTableDDLWithPsqlDbzStructRecord() throws IOException {
        // common
        String psqlDbzTopic = "debezium_postgresql.wdl_test.psql_common_table";
        String psqlDbzTable = "psql_common_table";
        // key
        String psqlDbzStructKeyString =
                "{\"schema\": {\"type\": \"struct\", \"fields\": [{\"type\": \"int32\", \"optional\": false, \"default\": 0, \"field\": \"id\"}], \"optional\": false, \"name\": \"debezium_postgresql.wdl_test.psql_common_table.Key\"}, \"payload\": {\"id\": 1}}";
        // value
        String psqlDbzStructNonKeyString =
                "{\"schema\": {\"type\": \"struct\", \"fields\": [{\"type\": \"struct\", \"fields\": [{\"type\": \"int32\", \"optional\": false, \"default\": 0, \"field\": \"id\"}, {\"type\": \"int16\", \"optional\": true, \"field\": \"col_smallint\"}, {\"type\": \"int32\", \"optional\": true, \"field\": \"col_integer\"}, {\"type\": \"int64\", \"optional\": true, \"field\": \"col_bigint\"}, {\"type\": \"bytes\", \"optional\": true, \"name\": \"org.apache.kafka.connect.data.Decimal\", \"version\": 1, \"parameters\": {\"scale\": \"2\", \"connect.decimal.precision\": \"10\"}, \"field\": \"col_numeric\"}, {\"type\": \"float\", \"optional\": true, \"field\": \"col_real\"}, {\"type\": \"double\", \"optional\": true, \"field\": \"col_double\"}, {\"type\": \"bytes\", \"optional\": true, \"name\": \"org.apache.kafka.connect.data.Decimal\", \"version\": 1, \"parameters\": {\"scale\": \"2\"}, \"field\": \"col_money\"}, {\"type\": \"string\", \"optional\": true, \"field\": \"col_varchar\"}, {\"type\": \"string\", \"optional\": true, \"field\": \"col_text\"}, {\"type\": \"int64\", \"optional\": true, \"name\": \"io.debezium.time.MicroTimestamp\", \"version\": 1, \"field\": \"col_timestamp\"}, {\"type\": \"string\", \"optional\": true, \"name\": \"io.debezium.time.ZonedTimestamp\", \"version\": 1, \"field\": \"col_timestamptz\"}, {\"type\": \"int32\", \"optional\": true, \"name\": \"io.debezium.time.Date\", \"version\": 1, \"field\": \"col_date\"}, {\"type\": \"int64\", \"optional\": true, \"name\": \"io.debezium.time.MicroTime\", \"version\": 1, \"field\": \"col_time\"}, {\"type\": \"int64\", \"optional\": true, \"name\": \"io.debezium.time.MicroDuration\", \"version\": 1, \"field\": \"col_interval\"}, {\"type\": \"boolean\", \"optional\": true, \"field\": \"col_boolean\"}, {\"type\": \"string\", \"optional\": true, \"name\": \"io.debezium.data.Uuid\", \"version\": 1, \"field\": \"col_uuid\"}, {\"type\": \"string\", \"optional\": true, \"name\": \"io.debezium.data.Json\", \"version\": 1, \"field\": \"col_json\"}, {\"type\": \"string\", \"optional\": true, \"name\": \"io.debezium.data.Json\", \"version\": 1, \"field\": \"col_jsonb\"}, {\"type\": \"bytes\", \"optional\": true, \"field\": \"col_bytea\"}, {\"type\": \"array\", \"items\": {\"type\": \"int32\", \"optional\": true}, \"optional\": true, \"field\": \"col_int_array\"}, {\"type\": \"array\", \"items\": {\"type\": \"string\", \"optional\": true}, \"optional\": true, \"field\": \"col_text_array\"}], \"optional\": true, \"name\": \"debezium_postgresql.wdl_test.psql_common_table.Value\", \"field\": \"before\"}, {\"type\": \"struct\", \"fields\": [{\"type\": \"int32\", \"optional\": false, \"default\": 0, \"field\": \"id\"}, {\"type\": \"int16\", \"optional\": true, \"field\": \"col_smallint\"}, {\"type\": \"int32\", \"optional\": true, \"field\": \"col_integer\"}, {\"type\": \"int64\", \"optional\": true, \"field\": \"col_bigint\"}, {\"type\": \"bytes\", \"optional\": true, \"name\": \"org.apache.kafka.connect.data.Decimal\", \"version\": 1, \"parameters\": {\"scale\": \"2\", \"connect.decimal.precision\": \"10\"}, \"field\": \"col_numeric\"}, {\"type\": \"float\", \"optional\": true, \"field\": \"col_real\"}, {\"type\": \"double\", \"optional\": true, \"field\": \"col_double\"}, {\"type\": \"bytes\", \"optional\": true, \"name\": \"org.apache.kafka.connect.data.Decimal\", \"version\": 1, \"parameters\": {\"scale\": \"2\"}, \"field\": \"col_money\"}, {\"type\": \"string\", \"optional\": true, \"field\": \"col_varchar\"}, {\"type\": \"string\", \"optional\": true, \"field\": \"col_text\"}, {\"type\": \"int64\", \"optional\": true, \"name\": \"io.debezium.time.MicroTimestamp\", \"version\": 1, \"field\": \"col_timestamp\"}, {\"type\": \"string\", \"optional\": true, \"name\": \"io.debezium.time.ZonedTimestamp\", \"version\": 1, \"field\": \"col_timestamptz\"}, {\"type\": \"int32\", \"optional\": true, \"name\": \"io.debezium.time.Date\", \"version\": 1, \"field\": \"col_date\"}, {\"type\": \"int64\", \"optional\": true, \"name\": \"io.debezium.time.MicroTime\", \"version\": 1, \"field\": \"col_time\"}, {\"type\": \"int64\", \"optional\": true, \"name\": \"io.debezium.time.MicroDuration\", \"version\": 1, \"field\": \"col_interval\"}, {\"type\": \"boolean\", \"optional\": true, \"field\": \"col_boolean\"}, {\"type\": \"string\", \"optional\": true, \"name\": \"io.debezium.data.Uuid\", \"version\": 1, \"field\": \"col_uuid\"}, {\"type\": \"string\", \"optional\": true, \"name\": \"io.debezium.data.Json\", \"version\": 1, \"field\": \"col_json\"}, {\"type\": \"string\", \"optional\": true, \"name\": \"io.debezium.data.Json\", \"version\": 1, \"field\": \"col_jsonb\"}, {\"type\": \"bytes\", \"optional\": true, \"field\": \"col_bytea\"}, {\"type\": \"array\", \"items\": {\"type\": \"int32\", \"optional\": true}, \"optional\": true, \"field\": \"col_int_array\"}, {\"type\": \"array\", \"items\": {\"type\": \"string\", \"optional\": true}, \"optional\": true, \"field\": \"col_text_array\"}], \"optional\": true, \"name\": \"debezium_postgresql.wdl_test.psql_common_table.Value\", \"field\": \"after\"}, {\"type\": \"struct\", \"fields\": [{\"type\": \"string\", \"optional\": false, \"field\": \"version\"}, {\"type\": \"string\", \"optional\": false, \"field\": \"connector\"}, {\"type\": \"string\", \"optional\": false, \"field\": \"name\"}, {\"type\": \"int64\", \"optional\": false, \"field\": \"ts_ms\"}, {\"type\": \"string\", \"optional\": true, \"name\": \"io.debezium.data.Enum\", \"version\": 1, \"parameters\": {\"allowed\": \"true,first,first_in_data_collection,last_in_data_collection,last,false,incremental\"}, \"default\": \"false\", \"field\": \"snapshot\"}, {\"type\": \"string\", \"optional\": false, \"field\": \"db\"}, {\"type\": \"string\", \"optional\": true, \"field\": \"sequence\"}, {\"type\": \"int64\", \"optional\": true, \"field\": \"ts_us\"}, {\"type\": \"int64\", \"optional\": true, \"field\": \"ts_ns\"}, {\"type\": \"string\", \"optional\": false, \"field\": \"schema\"}, {\"type\": \"string\", \"optional\": false, \"field\": \"table\"}, {\"type\": \"int64\", \"optional\": true, \"field\": \"txId\"}, {\"type\": \"int64\", \"optional\": true, \"field\": \"lsn\"}, {\"type\": \"int64\", \"optional\": true, \"field\": \"xmin\"}], \"optional\": false, \"name\": \"io.debezium.connector.postgresql.Source\", \"version\": 1, \"field\": \"source\"}, {\"type\": \"struct\", \"fields\": [{\"type\": \"string\", \"optional\": false, \"field\": \"id\"}, {\"type\": \"int64\", \"optional\": false, \"field\": \"total_order\"}, {\"type\": \"int64\", \"optional\": false, \"field\": \"data_collection_order\"}], \"optional\": true, \"name\": \"event.block\", \"version\": 1, \"field\": \"transaction\"}, {\"type\": \"string\", \"optional\": false, \"field\": \"op\"}, {\"type\": \"int64\", \"optional\": true, \"field\": \"ts_ms\"}, {\"type\": \"int64\", \"optional\": true, \"field\": \"ts_us\"}, {\"type\": \"int64\", \"optional\": true, \"field\": \"ts_ns\"}], \"optional\": false, \"name\": \"debezium_postgresql.wdl_test.psql_common_table.Envelope\", \"version\": 2}, \"payload\": {\"before\": null, \"after\": {\"id\": 1, \"col_smallint\": 32767, \"col_integer\": 2147483647, \"col_bigint\": 9223372036854775807, \"col_numeric\": \"AeJA\", \"col_real\": 1.23, \"col_double\": 4.5678, \"col_money\": \"ANMi\", \"col_varchar\": \"Debezium Compatible\", \"col_text\": \"Standard text field for CDC streaming.\", \"col_timestamp\": 1767600000000000, \"col_timestamptz\": \"2026-01-05T08:00:00.000000Z\", \"col_date\": 20458, \"col_time\": 28800000000, \"col_interval\": 93600000000, \"col_boolean\": true, \"col_uuid\": \"550e8400-e29b-41d4-a716-446655440000\", \"col_json\": \"{\\\"id\\\": 1, \\\"msg\\\": \\\"json\\\"}\", \"col_jsonb\": \"{\\\"tags\\\": [\\\"cdc\\\", \\\"kafka\\\"], \\\"status\\\": \\\"ok\\\"}\", \"col_bytea\": \"RGViZXppdW0=\", \"col_int_array\": [10, 20, 30], \"col_text_array\": [\"east\", \"west\"]}, \"source\": {\"version\": \"3.3.1.Final\", \"connector\": \"postgresql\", \"name\": \"wsl_dbz\", \"ts_ms\": 1767628768669, \"snapshot\": \"last_in_data_collection\", \"db\": \"sink\", \"sequence\": \"[null,\\\"75101088\\\"]\", \"ts_us\": 1767628768669672, \"ts_ns\": 1767628768669672000, \"schema\": \"public\", \"table\": \"debezium_compat_table\", \"txId\": 1032, \"lsn\": 75101088, \"xmin\": null}, \"transaction\": null, \"op\": \"r\", \"ts_ms\": 1767628768847, \"ts_us\": 1767628768847518, \"ts_ns\": 1767628768847518346}}";

        SchemaAndValue psqlDbzSchemaValueNonKey =
                jsonConverter.toConnectData(
                        psqlDbzTopic, psqlDbzStructNonKeyString.getBytes(StandardCharsets.UTF_8));

        SchemaAndValue psqlDbzSchemaValueKey =
                jsonConverter.toConnectData(
                        psqlDbzTopic, psqlDbzStructKeyString.getBytes(StandardCharsets.UTF_8));

        SinkRecord psqlDbzRecord =
                TestRecordBuffer.newSinkRecord(
                        psqlDbzTopic,
                        psqlDbzSchemaValueKey.schema(),
                        psqlDbzSchemaValueNonKey.schema(),
                        psqlDbzSchemaValueKey,
                        psqlDbzSchemaValueNonKey,
                        0);

        RecordDescriptor recordDescriptor =
                RecordDescriptor.builder()
                        .withSinkRecord(psqlDbzRecord)
                        .withTypeRegistry(recordTypeRegister.getTypeRegistry())
                        .build();

        String expectedDDL =
                "CREATE TABLE IF NOT EXISTS `default_cluster`.`psql_common_table` (`id` INT NOT NULL, `col_smallint` SMALLINT NULL, `col_integer` INT NULL, `col_bigint` BIGINT NULL, `col_numeric` DECIMAL(10,2) NULL, `col_real` FLOAT NULL, `col_double` DOUBLE NULL, `col_money` DECIMAL(38,2) NULL, `col_varchar` STRING NULL, `col_text` STRING NULL, `col_timestamp` DATETIME(6) NULL, `col_timestamptz` DATETIME(6) NULL, `col_date` DATE NULL, `col_time` DATETIME(6) NULL, `col_interval` BIGINT NULL, `col_boolean` BOOLEAN NULL, `col_uuid` STRING NULL, `col_json` JSON NULL, `col_jsonb` JSON NULL, `col_bytea` STRING NULL, `col_int_array` ARRAY<INT> NULL, `col_text_array` ARRAY<STRING> NULL) ENGINE=OLAP UNIQUE KEY (`id`) DISTRIBUTED BY HASH (`id`) BUCKETS AUTO PROPERTIES (\"replication_num\" = \"3\");";

        String ddlCreateTable =
                schemaCreationManager.buildCreateTableDDL(database, psqlDbzTable, recordDescriptor);

        Assert.assertEquals(expectedDDL, ddlCreateTable);
    }

    @Test
    public void buildCreateTableDDLWithMysqlDbzStructRecord() throws IOException {
        // Common
        String mysqlDbzTopic = "debezium_mysql.wdl_test.mysql_common_table";
        String mysqlDbzTable = "mysql_common_table";
        // Value
        String mysqlDbzStructNonKeyString =
                "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"age\"},{\"type\":\"string\",\"optional\":true,\"field\":\"email\"},{\"type\":\"int32\",\"optional\":true,\"name\":\"io.debezium.time.Date\",\"version\":1,\"field\":\"birth_date\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"integer_column\"},{\"type\":\"float\",\"optional\":true,\"field\":\"float_column\"},{\"type\":\"bytes\",\"optional\":true,\"name\":\"org.apache.kafka.connect.data.Decimal\",\"version\":1,\"parameters\":{\"scale\":\"2\",\"connect.decimal.precision\":\"10\"},\"field\":\"decimal_column\"},{\"type\":\"int64\",\"optional\":true,\"name\":\"io.debezium.time.Timestamp\",\"version\":1,\"field\":\"datetime_column\"},{\"type\":\"int32\",\"optional\":true,\"name\":\"io.debezium.time.Date\",\"version\":1,\"field\":\"date_column\"},{\"type\":\"int64\",\"optional\":true,\"name\":\"io.debezium.time.MicroTime\",\"version\":1,\"field\":\"time_column\"},{\"type\":\"string\",\"optional\":true,\"field\":\"text_column\"},{\"type\":\"string\",\"optional\":true,\"field\":\"varchar_column\"},{\"type\":\"bytes\",\"optional\":true,\"field\":\"binary_column\"},{\"type\":\"bytes\",\"optional\":true,\"field\":\"blob_column\"},{\"type\":\"int16\",\"optional\":true,\"field\":\"is_active\"}],\"optional\":true,\"name\":\"debezium_mysql.wdl_test.mysql_common_table.Value\",\"field\":\"before\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"age\"},{\"type\":\"string\",\"optional\":true,\"field\":\"email\"},{\"type\":\"int32\",\"optional\":true,\"name\":\"io.debezium.time.Date\",\"version\":1,\"field\":\"birth_date\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"integer_column\"},{\"type\":\"float\",\"optional\":true,\"field\":\"float_column\"},{\"type\":\"bytes\",\"optional\":true,\"name\":\"org.apache.kafka.connect.data.Decimal\",\"version\":1,\"parameters\":{\"scale\":\"2\",\"connect.decimal.precision\":\"10\"},\"field\":\"decimal_column\"},{\"type\":\"int64\",\"optional\":true,\"name\":\"io.debezium.time.Timestamp\",\"version\":1,\"field\":\"datetime_column\"},{\"type\":\"int32\",\"optional\":true,\"name\":\"io.debezium.time.Date\",\"version\":1,\"field\":\"date_column\"},{\"type\":\"int64\",\"optional\":true,\"name\":\"io.debezium.time.MicroTime\",\"version\":1,\"field\":\"time_column\"},{\"type\":\"string\",\"optional\":true,\"field\":\"text_column\"},{\"type\":\"string\",\"optional\":true,\"field\":\"varchar_column\"},{\"type\":\"bytes\",\"optional\":true,\"field\":\"binary_column\"},{\"type\":\"bytes\",\"optional\":true,\"field\":\"blob_column\"},{\"type\":\"int16\",\"optional\":true,\"field\":\"is_active\"}],\"optional\":true,\"name\":\"debezium_mysql.wdl_test.mysql_common_table.Value\",\"field\":\"after\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"version\"},{\"type\":\"string\",\"optional\":false,\"field\":\"connector\"},{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"ts_ms\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.data.Enum\",\"version\":1,\"parameters\":{\"allowed\":\"true,last,false,incremental\"},\"default\":\"false\",\"field\":\"snapshot\"},{\"type\":\"string\",\"optional\":false,\"field\":\"db\"},{\"type\":\"string\",\"optional\":true,\"field\":\"sequence\"},{\"type\":\"string\",\"optional\":true,\"field\":\"table\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"server_id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"gtid\"},{\"type\":\"string\",\"optional\":false,\"field\":\"file\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"pos\"},{\"type\":\"int32\",\"optional\":false,\"field\":\"row\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"thread\"},{\"type\":\"string\",\"optional\":true,\"field\":\"query\"}],\"optional\":false,\"name\":\"io.debezium.connector.mysql.Source\",\"field\":\"source\"},{\"type\":\"string\",\"optional\":false,\"field\":\"op\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_ms\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"id\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"total_order\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"data_collection_order\"}],\"optional\":true,\"name\":\"event.block\",\"version\":1,\"field\":\"transaction\"}],\"optional\":false,\"name\":\"debezium_mysql.wdl_test.mysql_common_table.Envelope\",\"version\":1},\"payload\":{\"before\":null,\"after\":{\"id\":8,\"name\":\"Jfohn"
                        + " Doe\",\"age\":430,\"email\":\"john@example.com\",\"birth_date\":8905,\"integer_column\":12323,\"float_column\":45.67,\"decimal_column\":\"MDk=\",\"datetime_column\":1712917800000,\"date_column\":19825,\"time_column\":37800000000,\"text_column\":\"Lorem"
                        + " ipsum dolor sit amet, consectetur adipiscing"
                        + " elit.\",\"varchar_column\":null,\"binary_column\":\"EjRWeJCrze8AAA==\",\"blob_column\":null,\"is_active\":2},\"source\":{\"version\":\"2.5.4.Final\",\"connector\":\"mysql\",\"name\":\"normal\",\"ts_ms\":1712915126000,\"snapshot\":\"false\",\"db\":\"wdl_test\",\"sequence\":null,\"table\":\"psql_common_table\",\"server_id\":1,\"gtid\":null,\"file\":\"binlog.000063\",\"pos\":13454,\"row\":0,\"thread\":20,\"query\":null},\"op\":\"c\",\"ts_ms\":1712915126481,\"transaction\":null}}";

        // Key
        String mysqlDbzStructKeyString =
                "{\"schema\": {\"type\": \"struct\", \"fields\": [{\"type\": \"int32\", \"optional\": false, \"default\": 0, \"field\": \"id\"}], \"optional\": false, \"name\": \"debezium_mysql.wdl_test.mysql_common_table.Key\"}, \"payload\": {\"id\": 8}}";
        ;

        SchemaAndValue mysqlDbzSchemaValueNonKey =
                jsonConverter.toConnectData(
                        mysqlDbzTopic, mysqlDbzStructNonKeyString.getBytes(StandardCharsets.UTF_8));

        SchemaAndValue mysqlDbzSchemaValueKey =
                jsonConverter.toConnectData(
                        mysqlDbzTopic, mysqlDbzStructKeyString.getBytes(StandardCharsets.UTF_8));

        SinkRecord mysqlDbzRecord =
                TestRecordBuffer.newSinkRecord(
                        mysqlDbzTopic,
                        mysqlDbzSchemaValueKey.schema(),
                        mysqlDbzSchemaValueNonKey.schema(),
                        mysqlDbzSchemaValueKey,
                        mysqlDbzSchemaValueNonKey,
                        0);

        RecordDescriptor recordDescriptor =
                RecordDescriptor.builder()
                        .withSinkRecord(mysqlDbzRecord)
                        .withTypeRegistry(recordTypeRegister.getTypeRegistry())
                        .build();

        String expectedDDL =
                "CREATE TABLE IF NOT EXISTS `default_cluster`.`mysql_common_table` (`id` INT NOT NULL, `name` STRING NULL, `age` INT NULL, `email` STRING NULL, `birth_date` DATE NULL, `integer_column` INT NULL, `float_column` FLOAT NULL, `decimal_column` DECIMAL(10,2) NULL, `datetime_column` DATETIME(6) NULL, `date_column` DATE NULL, `time_column` DATETIME(6) NULL, `text_column` STRING NULL, `varchar_column` STRING NULL, `binary_column` STRING NULL, `blob_column` STRING NULL, `is_active` SMALLINT NULL) ENGINE=OLAP UNIQUE KEY (`id`) DISTRIBUTED BY HASH (`id`) BUCKETS AUTO PROPERTIES (\"replication_num\" = \"3\");";

        String ddlCreateTable =
                schemaCreationManager.buildCreateTableDDL(
                        database, mysqlDbzTable, recordDescriptor);

        Assert.assertEquals(expectedDDL, ddlCreateTable);
    }

    @Test
    public void buildCreateTableDDLWithMysqlDbzGeoStructRecord() throws IOException {
        // Common
        String mysqlDbzTopic = "debezium_mysql.wdl_test.mysql_geo_table";
        String mysqlDbzTable = "mysql_geo_table";
        // Value
        String mysqlDbzStructNonKeyString =
                "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"double\",\"optional\":false,\"field\":\"x\"},{\"type\":\"double\",\"optional\":false,\"field\":\"y\"},{\"type\":\"bytes\",\"optional\":true,\"field\":\"wkb\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"srid\"}],\"optional\":true,\"name\":\"io.debezium.data.geometry.Point\",\"version\":1,\"doc\":\"Geometry"
                        + " (POINT)\",\"field\":\"geo_point\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"bytes\",\"optional\":false,\"field\":\"wkb\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"srid\"}],\"optional\":true,\"name\":\"io.debezium.data.geometry.Geometry\",\"version\":1,\"doc\":\"Geometry\",\"field\":\"geo_linestring\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"bytes\",\"optional\":false,\"field\":\"wkb\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"srid\"}],\"optional\":true,\"name\":\"io.debezium.data.geometry.Geometry\",\"version\":1,\"doc\":\"Geometry\",\"field\":\"geo_polygon\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"bytes\",\"optional\":false,\"field\":\"wkb\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"srid\"}],\"optional\":true,\"name\":\"io.debezium.data.geometry.Geometry\",\"version\":1,\"doc\":\"Geometry\",\"field\":\"geo_multipoint\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"bytes\",\"optional\":false,\"field\":\"wkb\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"srid\"}],\"optional\":true,\"name\":\"io.debezium.data.geometry.Geometry\",\"version\":1,\"doc\":\"Geometry\",\"field\":\"geo_multilinestring\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"bytes\",\"optional\":false,\"field\":\"wkb\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"srid\"}],\"optional\":true,\"name\":\"io.debezium.data.geometry.Geometry\",\"version\":1,\"doc\":\"Geometry\",\"field\":\"geo_multipolygon\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"bytes\",\"optional\":false,\"field\":\"wkb\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"srid\"}],\"optional\":true,\"name\":\"io.debezium.data.geometry.Geometry\",\"version\":1,\"doc\":\"Geometry\",\"field\":\"geo_geometry\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"bytes\",\"optional\":false,\"field\":\"wkb\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"srid\"}],\"optional\":true,\"name\":\"io.debezium.data.geometry.Geometry\",\"version\":1,\"doc\":\"Geometry\",\"field\":\"geo_geometrycollection\"}],\"optional\":true,\"name\":\"mysql_test.doris_test.geo_table.Value\",\"field\":\"before\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"double\",\"optional\":false,\"field\":\"x\"},{\"type\":\"double\",\"optional\":false,\"field\":\"y\"},{\"type\":\"bytes\",\"optional\":true,\"field\":\"wkb\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"srid\"}],\"optional\":true,\"name\":\"io.debezium.data.geometry.Point\",\"version\":1,\"doc\":\"Geometry"
                        + " (POINT)\",\"field\":\"geo_point\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"bytes\",\"optional\":false,\"field\":\"wkb\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"srid\"}],\"optional\":true,\"name\":\"io.debezium.data.geometry.Geometry\",\"version\":1,\"doc\":\"Geometry\",\"field\":\"geo_linestring\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"bytes\",\"optional\":false,\"field\":\"wkb\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"srid\"}],\"optional\":true,\"name\":\"io.debezium.data.geometry.Geometry\",\"version\":1,\"doc\":\"Geometry\",\"field\":\"geo_polygon\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"bytes\",\"optional\":false,\"field\":\"wkb\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"srid\"}],\"optional\":true,\"name\":\"io.debezium.data.geometry.Geometry\",\"version\":1,\"doc\":\"Geometry\",\"field\":\"geo_multipoint\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"bytes\",\"optional\":false,\"field\":\"wkb\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"srid\"}],\"optional\":true,\"name\":\"io.debezium.data.geometry.Geometry\",\"version\":1,\"doc\":\"Geometry\",\"field\":\"geo_multilinestring\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"bytes\",\"optional\":false,\"field\":\"wkb\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"srid\"}],\"optional\":true,\"name\":\"io.debezium.data.geometry.Geometry\",\"version\":1,\"doc\":\"Geometry\",\"field\":\"geo_multipolygon\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"bytes\",\"optional\":false,\"field\":\"wkb\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"srid\"}],\"optional\":true,\"name\":\"io.debezium.data.geometry.Geometry\",\"version\":1,\"doc\":\"Geometry\",\"field\":\"geo_geometry\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"bytes\",\"optional\":false,\"field\":\"wkb\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"srid\"}],\"optional\":true,\"name\":\"io.debezium.data.geometry.Geometry\",\"version\":1,\"doc\":\"Geometry\",\"field\":\"geo_geometrycollection\"}],\"optional\":true,\"name\":\"mysql_test.doris_test.geo_table.Value\",\"field\":\"after\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"version\"},{\"type\":\"string\",\"optional\":false,\"field\":\"connector\"},{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"ts_ms\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.data.Enum\",\"version\":1,\"parameters\":{\"allowed\":\"true,last,false,incremental\"},\"default\":\"false\",\"field\":\"snapshot\"},{\"type\":\"string\",\"optional\":false,\"field\":\"db\"},{\"type\":\"string\",\"optional\":true,\"field\":\"sequence\"},{\"type\":\"string\",\"optional\":true,\"field\":\"table\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"server_id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"gtid\"},{\"type\":\"string\",\"optional\":false,\"field\":\"file\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"pos\"},{\"type\":\"int32\",\"optional\":false,\"field\":\"row\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"thread\"},{\"type\":\"string\",\"optional\":true,\"field\":\"query\"}],\"optional\":false,\"name\":\"io.debezium.connector.mysql.Source\",\"field\":\"source\"},{\"type\":\"string\",\"optional\":false,\"field\":\"op\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_ms\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"id\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"total_order\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"data_collection_order\"}],\"optional\":true,\"field\":\"transaction\"}],\"optional\":false,\"name\":\"mysql_test.doris_test.geo_table.Envelope\"},\"payload\":{\"before\":null,\"after\":{\"id\":1,\"geo_point\":{\"x\":1.0,\"y\":1.0,\"wkb\":\"AQEAAAAAAAAAAADwPwAAAAAAAPA/\",\"srid\":null},\"geo_linestring\":{\"wkb\":\"AQIAAAADAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAPA/AAAAAAAA8D8AAAAAAAAAQAAAAAAAAABA\",\"srid\":null},\"geo_polygon\":{\"wkb\":\"AQMAAAABAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADwPwAAAAAAAPA/AAAAAAAA8D8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=\",\"srid\":null},\"geo_multipoint\":{\"wkb\":\"AQQAAAACAAAAAQEAAAAAAAAAAAAAAAAAAAAAAAAAAQEAAAAAAAAAAADwPwAAAAAAAPA/\",\"srid\":null},\"geo_multilinestring\":{\"wkb\":\"AQUAAAACAAAAAQIAAAACAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAPA/AAAAAAAA8D8BAgAAAAIAAAAAAAAAAAAAQAAAAAAAAABAAAAAAAAACEAAAAAAAAAIQA==\",\"srid\":null},\"geo_multipolygon\":{\"wkb\":\"AQYAAAACAAAAAQMAAAABAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADwPwAAAAAAAPA/AAAAAAAA8D8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAwAAAAEAAAAEAAAAAAAAAAAAAEAAAAAAAAAAQAAAAAAAAAhAAAAAAAAACEAAAAAAAAAIQAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAQA==\",\"srid\":null},\"geo_geometry\":{\"wkb\":\"AQcAAAACAAAAAQEAAAAAAAAAAADwPwAAAAAAAPA/AQIAAAACAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAPA/AAAAAAAA8D8=\",\"srid\":null},\"geo_geometrycollection\":{\"wkb\":\"AQcAAAADAAAAAQEAAAAAAAAAAAAAQAAAAAAAAABAAQIAAAACAAAAAAAAAAAAAEAAAAAAAAAAQAAAAAAAAAhAAAAAAAAACEABAwAAAAEAAAAEAAAAAAAAAAAAAEAAAAAAAAAAQAAAAAAAAAhAAAAAAAAACEAAAAAAAAAIQAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAQA==\",\"srid\":null}},\"source\":{\"version\":\"1.9.8.Final\",\"connector\":\"mysql\",\"name\":\"mysql_test\",\"ts_ms\":1717483867000,\"snapshot\":\"false\",\"db\":\"doris_test\",\"sequence\":null,\"table\":\"geo_table\",\"server_id\":1,\"gtid\":null,\"file\":\"mysql-bin.000366\",\"pos\":896,\"row\":0,\"thread\":6,\"query\":null},\"op\":\"c\",\"ts_ms\":1717483867044,\"transaction\":null}}";

        // Key
        String mysqlDbzStructKeyString =
                "{\"schema\": {\"type\": \"struct\", \"fields\": [{\"type\": \"int32\", \"optional\": false, \"default\": 0, \"field\": \"id\"}], \"optional\": false, \"name\": \"debezium_mysql.wdl_test.mysql_geo_table.Key\"}, \"payload\": {\"id\": 1}}";
        ;

        SchemaAndValue mysqlDbzSchemaValueNonKey =
                jsonConverter.toConnectData(
                        mysqlDbzTopic, mysqlDbzStructNonKeyString.getBytes(StandardCharsets.UTF_8));

        SchemaAndValue mysqlDbzSchemaValueKey =
                jsonConverter.toConnectData(
                        mysqlDbzTopic, mysqlDbzStructKeyString.getBytes(StandardCharsets.UTF_8));

        SinkRecord mysqlDbzRecord =
                TestRecordBuffer.newSinkRecord(
                        mysqlDbzTopic,
                        mysqlDbzSchemaValueKey.schema(),
                        mysqlDbzSchemaValueNonKey.schema(),
                        mysqlDbzSchemaValueKey,
                        mysqlDbzSchemaValueNonKey,
                        0);

        RecordDescriptor recordDescriptor =
                RecordDescriptor.builder()
                        .withSinkRecord(mysqlDbzRecord)
                        .withTypeRegistry(recordTypeRegister.getTypeRegistry())
                        .build();

        String expectedDDL =
                "CREATE TABLE IF NOT EXISTS `default_cluster`.`mysql_geo_table` (`id` INT NOT NULL, `geo_point` STRING NULL, `geo_linestring` STRING NULL, `geo_polygon` STRING NULL, `geo_multipoint` STRING NULL, `geo_multilinestring` STRING NULL, `geo_multipolygon` STRING NULL, `geo_geometry` STRING NULL, `geo_geometrycollection` STRING NULL) ENGINE=OLAP UNIQUE KEY (`id`) DISTRIBUTED BY HASH (`id`) BUCKETS AUTO PROPERTIES (\"replication_num\" = \"3\");";

        String ddlCreateTable =
                schemaCreationManager.buildCreateTableDDL(
                        database, mysqlDbzTable, recordDescriptor);

        Assert.assertEquals(expectedDDL, ddlCreateTable);
    }
}
