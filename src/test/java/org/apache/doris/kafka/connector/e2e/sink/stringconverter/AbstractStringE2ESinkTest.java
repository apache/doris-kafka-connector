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

package org.apache.doris.kafka.connector.e2e.sink.stringconverter;

import java.util.Objects;
import java.util.Properties;
import org.apache.doris.kafka.connector.e2e.sink.AbstractKafka2DorisSink;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Produce messages using the {@link org.apache.kafka.common.serialization.StringSerializer}
 * serializer, Use the {@link org.apache.kafka.connect.storage.StringConverter} converter to consume
 * messages.
 */
public abstract class AbstractStringE2ESinkTest extends AbstractKafka2DorisSink {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractStringE2ESinkTest.class);
    private static KafkaProducer<String, String> producer;

    public static void initProducer() {
        if (Objects.nonNull(producer)) {
            return;
        }
        // Producer properties
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaInstanceHostAndPort);
        producerProperties.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(producerProperties);
        LOG.info("kafka producer started successfully.");
    }

    protected void produceMsg2Kafka(String topic, String value) {
        LOG.info("Kafka producer will produce msg. topic={}, msg={}", topic, value);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, value);
        producer.send(record);
        LOG.info("Kafka producer produced msg successfully. topic={}, msg={}", topic, value);
    }
}
