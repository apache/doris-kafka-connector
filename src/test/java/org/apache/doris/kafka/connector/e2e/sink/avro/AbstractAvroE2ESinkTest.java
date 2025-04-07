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

package org.apache.doris.kafka.connector.e2e.sink.avro;

import java.util.Objects;
import java.util.Properties;
import org.apache.avro.generic.GenericRecord;
import org.apache.doris.kafka.connector.e2e.sink.AbstractKafka2DorisSink;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Produce messages with avro. 1.Manage your own schema 2.Use a schema registry */
public abstract class AbstractAvroE2ESinkTest extends AbstractKafka2DorisSink {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractAvroE2ESinkTest.class);
    private static KafkaProducer<String, byte[]> producer;
    private static KafkaProducer<String, GenericRecord> avroProducer;

    public static void initByteProducer() {
        if (Objects.nonNull(producer)) {
            return;
        }
        // Producer properties
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaInstanceHostAndPort);
        producerProperties.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        producerProperties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.ByteArraySerializer.class);
        producer = new KafkaProducer<>(producerProperties);
        LOG.info("kafka producer started successfully.");
    }

    public static void initAvroProducer() {
        if (Objects.nonNull(avroProducer)) {
            return;
        }
        // Producer properties
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaInstanceHostAndPort);
        producerProperties.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        producerProperties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        producerProperties.put("schema.registry.url", getSchemaRegistryUrl());
        avroProducer = new KafkaProducer<>(producerProperties);
        LOG.info("kafka avro producer started successfully.");
    }

    protected void produceMsg2Kafka(String topic, GenericRecord value) {
        LOG.info("Kafka avro producer will produce msg. topic={}, msg={}", topic, value);
        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(topic, value);
        avroProducer.send(
                record,
                (recordMetadata, e) ->
                        LOG.info(
                                "Send avro Callback is {}, with error is ",
                                recordMetadata.offset(),
                                e));
        LOG.info("Kafka avro producer produced msg successfully. topic={}, msg={}", topic, value);
    }

    protected void produceMsg2Kafka(String topic, byte[] value) {
        LOG.info("Kafka producer will produce msg. topic={}, msg={}", topic, value);

        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, value);
        producer.send(
                record,
                (recordMetadata, e) ->
                        LOG.info(
                                "Send Callback is {}, with error is ", recordMetadata.offset(), e));

        LOG.info("Kafka producer produced msg successfully. topic={}, msg={}", topic, value);
    }
}
