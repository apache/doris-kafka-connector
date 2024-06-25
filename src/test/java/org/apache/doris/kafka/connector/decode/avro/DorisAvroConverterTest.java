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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DorisAvroConverterTest {
    private static final String USER_TOPIC = "user-topic";
    private static final String PRODUCT_TOPIC = "product-topic";
    private static final String USER_AVRO_PATH = "decode/avro/user.avsc";
    private static final String PRODUCT_AVRO_PATH = "decode/avro/product.avsc";
    private final DorisAvroConverter avroConverter = new DorisAvroConverter();
    private final Map<String, String> configs = new HashMap<>();

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
        configs.put(DorisAvroConverter.AVRO_CONVERTER_TEST, "true");
    }

    @Test
    public void testParseTopicSchema() throws IOException {
        avroConverter.parseTopic2Schema(configs);
        Map<String, Schema> topic2SchemaMap = avroConverter.getTopic2SchemaMap();

        Assert.assertTrue(topic2SchemaMap.containsKey(USER_TOPIC));
        Assert.assertTrue(topic2SchemaMap.containsKey(PRODUCT_TOPIC));

        String userPath =
                Objects.requireNonNull(this.getClass().getClassLoader().getResource(USER_AVRO_PATH))
                        .getPath();
        String productPath =
                Objects.requireNonNull(
                                this.getClass().getClassLoader().getResource(PRODUCT_AVRO_PATH))
                        .getPath();
        Schema productSchema = new Schema.Parser().parse(new File(productPath));
        Schema userSchema = new Schema.Parser().parse(new File(userPath));
        Assert.assertEquals(topic2SchemaMap.get(USER_TOPIC), userSchema);
        Assert.assertEquals(topic2SchemaMap.get(PRODUCT_TOPIC), productSchema);
    }
}
