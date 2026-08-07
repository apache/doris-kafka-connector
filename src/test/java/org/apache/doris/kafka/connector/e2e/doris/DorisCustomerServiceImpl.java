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

package org.apache.doris.kafka.connector.e2e.doris;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.kafka.connector.cfg.DorisTlsOptions;
import org.apache.doris.kafka.connector.connection.DorisJdbcTlsAdapter;
import org.apache.doris.kafka.connector.exception.DorisException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Uses a Doris cluster supplied through system properties for connector E2E tests. */
public class DorisCustomerServiceImpl implements DorisContainerService {

    public static final String CUSTOMER_ENV = "customer_env";
    public static final String DORIS_HOST = "doris_host";
    public static final String DORIS_QUERY_PORT = "doris_query_port";
    public static final String DORIS_HTTP_PORT = "doris_http_port";
    public static final String DORIS_USER = "doris_user";
    public static final String DORIS_PASSWORD = "doris_passwd";
    public static final String DORIS_ENABLE_TLS = "doris_enable_tls";
    public static final String DORIS_TLS_CA_CERTIFICATE_PATH = "doris_tls_ca_certificate_path";
    public static final String DORIS_TLS_SKIP_HOSTNAME_VERIFICATION =
            "doris_tls_skip_hostname_verification";
    public static final String DORIS_TLS_EXCLUDED_PROTOCOLS = "doris_tls_excluded_protocols";

    private static final Logger LOG = LoggerFactory.getLogger(DorisCustomerServiceImpl.class);
    private static final String JDBC_URL = "jdbc:mysql://%s:%s";

    @Override
    public void startContainer() {
        validateProperties();
        if (!isRunning()) {
            throw new DorisException("No alive backend was found in the external Doris cluster");
        }
        LOG.info("Using external Doris cluster at {}:{}", getInstanceHost(), getHttpPort());
    }

    @Override
    public Connection getQueryConnection() {
        DorisJdbcTlsAdapter adapter = null;
        try {
            DorisTlsOptions tlsOptions = getTlsOptions();
            adapter = DorisJdbcTlsAdapter.create(tlsOptions);
            Properties properties =
                    adapter.createConnectionProperties(getUsername(), getPassword());
            Connection connection =
                    DriverManager.getConnection(
                            String.format(JDBC_URL, getInstanceHost(), getQueryPort()), properties);
            // The truststore is only needed while Connector/J establishes the TLS session.
            adapter.close();
            return connection;
        } catch (SQLException | RuntimeException e) {
            if (adapter != null) {
                adapter.close();
            }
            throw new DorisException("Failed to connect to the external Doris cluster", e);
        }
    }

    @Override
    public String getInstanceHost() {
        return System.getProperty(DORIS_HOST);
    }

    @Override
    public int getHttpPort() {
        return Integer.parseInt(System.getProperty(DORIS_HTTP_PORT));
    }

    @Override
    public int getQueryPort() {
        return Integer.parseInt(System.getProperty(DORIS_QUERY_PORT));
    }

    @Override
    public String getUsername() {
        return System.getProperty(DORIS_USER);
    }

    @Override
    public String getPassword() {
        return System.getProperty(DORIS_PASSWORD);
    }

    @Override
    public DorisTlsOptions getTlsOptions() {
        return DorisTlsOptions.builder()
                .setEnabled(Boolean.parseBoolean(System.getProperty(DORIS_ENABLE_TLS, "false")))
                .setCaCertificatePath(System.getProperty(DORIS_TLS_CA_CERTIFICATE_PATH, ""))
                .setSkipHostnameVerification(
                        Boolean.parseBoolean(
                                System.getProperty(DORIS_TLS_SKIP_HOSTNAME_VERIFICATION, "false")))
                .setExcludedProtocols(System.getProperty(DORIS_TLS_EXCLUDED_PROTOCOLS, ""))
                .build();
    }

    @Override
    public void close() {}

    void validateProperties() {
        requireProperty(DORIS_HOST);
        requireProperty(DORIS_QUERY_PORT);
        requireProperty(DORIS_HTTP_PORT);
        requireProperty(DORIS_USER);
        if (System.getProperty(DORIS_PASSWORD) == null) {
            throw new IllegalArgumentException(DORIS_PASSWORD + " is required");
        }
        getQueryPort();
        getHttpPort();

        DorisTlsOptions tlsOptions = getTlsOptions();
        if (tlsOptions.isEnabled()
                && StringUtils.isNotBlank(tlsOptions.getCaCertificatePath())
                && !Files.isReadable(Paths.get(tlsOptions.getCaCertificatePath()))) {
            throw new IllegalArgumentException(
                    "Doris TLS CA certificate is not readable: "
                            + tlsOptions.getCaCertificatePath());
        }
    }

    private boolean isRunning() {
        try (Connection connection = getQueryConnection();
                Statement statement = connection.createStatement();
                ResultSet backends = statement.executeQuery("SHOW BACKENDS")) {
            while (backends.next()) {
                if (Boolean.parseBoolean(backends.getString("Alive").trim())) {
                    return true;
                }
            }
        } catch (SQLException | RuntimeException e) {
            LOG.error("Failed to inspect the external Doris cluster", e);
        }
        return false;
    }

    private static void requireProperty(String name) {
        if (StringUtils.isBlank(System.getProperty(name))) {
            throw new IllegalArgumentException(name + " is required");
        }
    }
}
