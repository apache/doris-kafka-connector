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

package org.apache.doris.kafka.connector.decode.avro;

import com.google.common.annotations.VisibleForTesting;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.doris.kafka.connector.decode.DorisConverter;
import org.apache.doris.kafka.connector.decode.DorisJsonSchema;
import org.apache.doris.kafka.connector.exception.DataDecodeException;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Supports specifying the avsc file of avro, can directly obtain the avro schema by parsing the
 * file.
 */
public class DorisAvroConverter extends DorisConverter {
    public static final String AVRO_TOPIC_SCHEMA_FILEPATH = "avro.topic2schema.filepath";
    private static final Logger LOG = LoggerFactory.getLogger(DorisAvroConverter.class);
    private final Map<String, Schema> topic2SchemaMap = new HashMap<>();

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        parseTopic2Schema(configs);
    }

    @VisibleForTesting
    public void parseTopic2Schema(final Map<String, ?> configs) {
        Object avroSchemaPath = configs.get(AVRO_TOPIC_SCHEMA_FILEPATH);

        if (avroSchemaPath == null) {
            LOG.error(AVRO_TOPIC_SCHEMA_FILEPATH + " can not be empty in DorisAvroConverter.class");
            throw new DataDecodeException(
                    AVRO_TOPIC_SCHEMA_FILEPATH + " can not be empty in DorisAvroConverter.class");
        }

        if (avroSchemaPath instanceof String) {
            Map<String, String> topic2SchemaFileMap = parseTopicSchemaPath((String) avroSchemaPath);
            for (Map.Entry<String, String> entry : topic2SchemaFileMap.entrySet()) {
                String topic = entry.getKey();
                String schemaPath = entry.getValue();
                Schema schema;
                try {
                    schema = new Schema.Parser().parse(new File(schemaPath));
                } catch (SchemaParseException | IOException e) {
                    LOG.error(
                            "the provided for "
                                    + AVRO_TOPIC_SCHEMA_FILEPATH
                                    + " is no valid, failed to parse {} {}",
                            topic,
                            schemaPath,
                            e);
                    throw new DataDecodeException(
                            "the provided for "
                                    + AVRO_TOPIC_SCHEMA_FILEPATH
                                    + " is no valid, failed to parse "
                                    + topic
                                    + " "
                                    + schemaPath
                                    + ".\n",
                            e);
                }
                topic2SchemaMap.put(topic, schema);
            }
        } else {
            LOG.error(AVRO_TOPIC_SCHEMA_FILEPATH + " must be a string.");
            throw new DataDecodeException(
                    "The "
                            + AVRO_TOPIC_SCHEMA_FILEPATH
                            + " is provided, but can not be parsed as an Avro schema.");
        }
    }

    /**
     * Parse the mapping between topic and schema file paths.
     *
     * @param topicSchemaPath topic1:file:///schema_test.avsc,topic2:file:///schema_test2.avsc
     */
    private Map<String, String> parseTopicSchemaPath(String topicSchemaPath) {
        Map<String, String> topic2SchemaPathMap = new HashMap<>();
        boolean isInvalid = false;
        for (String s : topicSchemaPath.split(",")) {
            String[] split = s.split(":file://");
            if (split.length != 2 || split[0].trim().isEmpty() || split[1].trim().isEmpty()) {
                LOG.error(
                        "Invalid {} config format: {}",
                        AVRO_TOPIC_SCHEMA_FILEPATH,
                        topicSchemaPath);
                isInvalid = true;
            }

            String topic = split[0].trim();
            String schemaPath = split[1].trim();

            if (topic2SchemaPathMap.containsKey(topic)) {
                LOG.error("topic name {} is duplicated.", topic);
                isInvalid = true;
            }
            topic2SchemaPathMap.put(topic, schemaPath);
        }
        if (isInvalid) {
            throw new DataDecodeException("Failed to parse " + AVRO_TOPIC_SCHEMA_FILEPATH + " map");
        }
        return topic2SchemaPathMap;
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        if (value == null) {
            LOG.warn("cast bytes is null");
            return new SchemaAndValue(new DorisJsonSchema(), null);
        }

        if (topic2SchemaMap.containsKey(topic)) {
            Schema schema = topic2SchemaMap.get(topic);
            ByteBuffer buffer = ByteBuffer.wrap(value);
            int length = buffer.limit();
            byte[] data = new byte[length];
            buffer.get(data, 0, length);
            try {
                return new SchemaAndValue(
                        new DorisJsonSchema(), parseAvroWithSchema(data, schema, schema));
            } catch (IOException e) {
                LOG.error("failed to parse AVRO record\n" + e);
                throw new DataDecodeException("failed to parse AVRO record\n", e);
            }
        } else {
            LOG.error("The avro schema file of {} is not provided.", topic);
            throw new DataDecodeException("The avro schema file of " + topic + " is not provided.");
        }
    }

    /**
     * Parse Avro record with a writer schema and a reader schema. The writer and the reader schema
     * have to be compatible as described in
     * https://avro.apache.org/docs/1.9.2/spec.html#Schema+Resolution
     *
     * @param data avro data
     * @param writerSchema avro schema with which data got serialized
     * @param readerSchema avro schema with which will to be read and returned
     */
    private String parseAvroWithSchema(final byte[] data, Schema writerSchema, Schema readerSchema)
            throws IOException {
        final GenericData genericData = new GenericData();
        // Conversion for logical type Decimal.
        // There are conversions for other logical types as well.
        genericData.addLogicalTypeConversion(new Conversions.DecimalConversion());

        InputStream is = new ByteArrayInputStream(data);
        Decoder decoder = DecoderFactory.get().binaryDecoder(is, null);
        DatumReader<GenericRecord> reader =
                new GenericDatumReader<>(writerSchema, readerSchema, genericData);
        GenericRecord datum = reader.read(null, decoder);
        return datum.toString();
    }

    @VisibleForTesting
    public Map<String, Schema> getTopic2SchemaMap() {
        return topic2SchemaMap;
    }
}
