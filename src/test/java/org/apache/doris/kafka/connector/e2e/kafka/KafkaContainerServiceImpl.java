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

package org.apache.doris.kafka.connector.e2e.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.doris.kafka.connector.exception.DorisException;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.kafka.connect.cli.ConnectDistributed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

public class KafkaContainerServiceImpl implements KafkaContainerService {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaContainerServiceImpl.class);
    private static final String KAFKA_IMAGE = "confluentinc/cp-kafka:7.6.1";
    private static final String CONNECT_PROPERTIES_PATH =
            Objects.requireNonNull(
                            KafkaContainerServiceImpl.class
                                    .getClassLoader()
                                    .getResource("connect-distributed.properties"))
                    .getPath();
    private static final String NEW_CONNECT_PROPERTIES =
            "src/test/resources/new-connect-distributed.properties";
    private KafkaContainer kafkaContainer;
    private final CloseableHttpClient httpClient = HttpClients.createDefault();
    private String kafkaServerHost;
    private int kafkaServerPort;
    private static final String CONNECT_PORT = "8083";
    private static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private static final int MAX_RETRIES = 5;

    @Override
    public String getInstanceHostAndPort() {
        return kafkaServerHost + ":" + kafkaServerPort;
    }

    public void startConnector() {
        LOG.info("Doris-kafka-connect will be starting.");
        try {
            String[] params = new String[1];
            params[0] = getConnectPropertiesPath();
            // Start ConnectDistributed and run it in a separate thread to prevent blocking.
            executorService.submit(() -> ConnectDistributed.main(params));
            LOG.info("kafka-connect has been submitted to start.");
            Thread.sleep(10000);
        } catch (Exception e) {
            LOG.error("Failed to start doris-kafka-connect.", e);
        }
        waitForKafkaConnect();
    }

    public void waitForKafkaConnect() {
        String kafkaConnectUrl = "http://" + kafkaServerHost + ":" + CONNECT_PORT + "/connectors";
        int responseCode = -1;
        int attempts = 0;
        while (attempts < MAX_RETRIES) {
            try {
                URL url = new URL(kafkaConnectUrl);
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("GET");
                connection.setConnectTimeout(5000);
                connection.setReadTimeout(5000);

                responseCode = connection.getResponseCode();
                if (responseCode == 200) {
                    LOG.info("doris-kafka-connect is up and running on " + kafkaConnectUrl);
                    return;
                }
                LOG.info(
                        "Received response code "
                                + responseCode
                                + ". Waiting for doris-kafka-connect to be ready.");
            } catch (IOException e) {
                LOG.info("Failed to connect to " + kafkaConnectUrl + ". Retrying...");
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.error("Thread interrupted while waiting for Kafka Connect to start.", e);
            }
            attempts++;
        }
        LOG.error("doris-kafka-connect did not start within " + MAX_RETRIES + " attempts.");
    }

    /**
     * After the container containing the kafka server is started, the externally mapped port is not
     * 9092. To keep the real kafka 9092 mapped port consistent with the connect registered port.
     */
    private String getConnectPropertiesPath() {
        Properties properties = new Properties();
        try (InputStream fis = Files.newInputStream(Paths.get(CONNECT_PROPERTIES_PATH))) {
            properties.load(fis);
        } catch (IOException e) {
            throw new DorisException(
                    "Failed to read " + CONNECT_PROPERTIES_PATH + "properties file.", e);
        }
        String bootstrapServers = kafkaServerHost + ":" + kafkaServerPort;
        properties.put(BOOTSTRAP_SERVERS, bootstrapServers);
        LOG.info("The bootstrap.servers set to {}", bootstrapServers);

        try (OutputStream fos = Files.newOutputStream(Paths.get(NEW_CONNECT_PROPERTIES))) {
            properties.store(fos, "Updated Kafka Connect Properties.");
        } catch (IOException e) {
            throw new DorisException("Failed to write properties file", e);
        }
        return NEW_CONNECT_PROPERTIES;
    }

    @Override
    public void startContainer() {
        LOG.info("kafka server is about to be initialized.");
        kafkaContainer = new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE));

        kafkaContainer.start();
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new DorisException(e);
        }
        kafkaServerHost = kafkaContainer.getHost();
        kafkaServerPort = kafkaContainer.getMappedPort(9093);
        LOG.info(
                "kafka server started successfully. instance={}",
                kafkaContainer.getBootstrapServers());
        LOG.info(
                "kafka server started successfully. instanceHost={}, instancePort={}",
                kafkaServerHost,
                kafkaServerPort);
    }

    @Override
    public void close() {
        LOG.info("Kafka server is about to be shut down.");
        shutdownConnector();
        kafkaContainer.close();
        LOG.info("Kafka server shuts down successfully.");
    }

    private void shutdownConnector() {
        try {
            LOG.info("Shutting down ExecutorService.");
            executorService.shutdown();
            if (!executorService.awaitTermination(60, java.util.concurrent.TimeUnit.SECONDS)) {
                executorService.shutdownNow();
                if (!executorService.awaitTermination(60, java.util.concurrent.TimeUnit.SECONDS)) {
                    LOG.error("ExecutorService did not terminate.");
                }
            }
        } catch (InterruptedException ie) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void registerKafkaConnector(String name, String msg)
            throws IOException, InterruptedException {
        LOG.info("{} Kafka connector will be registering, bodyMsg={}", name, msg);
        String connectUrl = "http://" + kafkaServerHost + ":" + CONNECT_PORT + "/connectors";
        HttpPost httpPost = new HttpPost(connectUrl);
        StringEntity entity = new StringEntity(msg);
        httpPost.setEntity(entity);
        httpPost.setHeader("Content-type", "application/json");
        try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
            StatusLine statusLine = response.getStatusLine();
            if (statusLine.getStatusCode() != 201) {
                LOG.warn(
                        "Failed to register {} kafka connect, msg={}",
                        name,
                        statusLine.getReasonPhrase());
            }
        } catch (IOException e) {
            LOG.warn("Failed to delete kafka connect, name={}", name);
        }
        LOG.info("{} Kafka connector registered successfully.", name);

        // The current thread sleeps for 10 seconds so that connect can consume messages to doris in
        // time.
        Thread.sleep(10000);
    }

    @Override
    public void deleteKafkaConnector(String name) {
        LOG.info("{} Kafka connector will be deleting.", name);
        String connectUrl = "http://" + kafkaServerHost + ":" + CONNECT_PORT + "/connectors/";
        String deleteUrl = connectUrl + name;
        HttpDelete httpDelete = new HttpDelete(deleteUrl);
        try (CloseableHttpResponse response = httpClient.execute(httpDelete)) {
            StatusLine statusLine = response.getStatusLine();
            if (statusLine.getStatusCode() != 204) {
                LOG.warn(
                        "Failed to delete {} kafka connect, msg={}",
                        name,
                        statusLine.getReasonPhrase());
            }
        } catch (IOException e) {
            LOG.warn("Failed to delete kafka connect, name={}", name);
        }
        LOG.info("{} Kafka connector deleted successfully.", name);
    }
}
