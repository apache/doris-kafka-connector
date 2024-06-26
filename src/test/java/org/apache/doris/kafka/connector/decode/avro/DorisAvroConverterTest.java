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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DorisAvroConverterTest {
    private static final String USER_TOPIC = "user-topic";
    private static final String PRODUCT_TOPIC = "product-topic";
    private static final String USER_AVRO_PATH = "src/test/resources/decode/avro/user.avsc";
    private static final String PRODUCT_AVRO_PATH = "src/test/resources/decode/avro/product.avsc";
    private final DorisAvroConverter avroConverter = new DorisAvroConverter();
    private final Map<String, String> configs = new HashMap<>();
    private final List<byte[]> records = new ArrayList<>();

    @Before
    public void init() {
        String topic2SchemaPath =
                USER_TOPIC
                        + ":file://"
                        + USER_AVRO_PATH
                        + ", "
                        + PRODUCT_TOPIC
                        + ":file://"
                        + PRODUCT_AVRO_PATH;
        configs.put(DorisAvroConverter.AVRO_TOPIC_SCHEMA_FILEPATH, topic2SchemaPath);
    }

    @Test
    public void testParseTopicSchema() throws IOException {
        avroConverter.parseTopic2Schema(configs);
        Map<String, Schema> topic2SchemaMap = avroConverter.getTopic2SchemaMap();

        Assert.assertTrue(topic2SchemaMap.containsKey(USER_TOPIC));
        Assert.assertTrue(topic2SchemaMap.containsKey(PRODUCT_TOPIC));

        Schema productSchema = new Schema.Parser().parse(new File(PRODUCT_AVRO_PATH));
        Schema userSchema = new Schema.Parser().parse(new File(USER_AVRO_PATH));
        Assert.assertEquals(topic2SchemaMap.get(USER_TOPIC), userSchema);
        Assert.assertEquals(topic2SchemaMap.get(PRODUCT_TOPIC), productSchema);
    }

    @Test
    public void testConvert() throws IOException {
        Schema userSchema = new Schema.Parser().parse(new File(USER_AVRO_PATH));
        byte[] userAvroData = generateUserRecord(userSchema);
        avroConverter.parseTopic2Schema(configs);
        SchemaAndValue schemaAndValue = avroConverter.toConnectData("user-topic", userAvroData);
        Object o = schemaAndValue.value();

        Assert.assertEquals(o, "{\"id\": 1, \"name\": \"test\", \"age\": 18}");
    }

    private byte[] generateUserRecord(Schema userSchema) throws IOException {
        GenericData.Record userRecord = new GenericData.Record(userSchema);
        userRecord.put("id", 1);
        userRecord.put("name", "test");
        userRecord.put("age", 18);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        DatumWriter<GenericRecord> writer = new SpecificDatumWriter<GenericRecord>(userSchema);
        writer.write(userRecord, encoder);
        encoder.flush();
        return outputStream.toByteArray();
    }
}
