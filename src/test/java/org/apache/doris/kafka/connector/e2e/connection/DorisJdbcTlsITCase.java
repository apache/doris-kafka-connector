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

package org.apache.doris.kafka.connector.e2e.connection;

import com.mysql.cj.jdbc.JdbcConnection;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import org.apache.doris.kafka.connector.cfg.DorisOptions;
import org.apache.doris.kafka.connector.cfg.DorisSinkConnectorConfig;
import org.apache.doris.kafka.connector.cfg.DorisTlsOptions;
import org.apache.doris.kafka.connector.connection.JdbcConnectionProvider;
import org.apache.doris.kafka.connector.e2e.doris.DorisCustomerServiceImpl;
import org.apache.doris.kafka.connector.utils.ConfigCheckUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

/** Verifies the production JDBC provider against an externally supplied TLS Doris cluster. */
public class DorisJdbcTlsITCase {

    private static DorisOptions dorisOptions;

    @BeforeClass
    public static void setUp() {
        Assume.assumeTrue(
                "External Doris is required",
                Boolean.parseBoolean(
                        System.getProperty(DorisCustomerServiceImpl.CUSTOMER_ENV, "false")));
        Assume.assumeTrue(
                "External Doris TLS is required",
                Boolean.parseBoolean(
                        System.getProperty(DorisCustomerServiceImpl.DORIS_ENABLE_TLS, "false")));

        dorisOptions = createDorisOptions();
        Assume.assumeTrue(
                "Doris MySQL TLS must not be excluded",
                dorisOptions.getTlsOptions().isEnabledFor(DorisTlsOptions.Protocol.MYSQL));
    }

    @Test
    public void testJdbcProviderEstablishesTlsConnection() throws Exception {
        JdbcConnectionProvider provider = new JdbcConnectionProvider(dorisOptions);
        try {
            Connection connection = provider.getOrEstablishConnection();
            JdbcConnection mysqlConnection = connection.unwrap(JdbcConnection.class);
            Assert.assertTrue(mysqlConnection.getSession().isSSLEstablished());

            try (Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery("SELECT 1")) {
                Assert.assertTrue(resultSet.next());
                Assert.assertEquals(1, resultSet.getInt(1));
                Assert.assertFalse(resultSet.next());
            }
        } finally {
            provider.closeConnection();
        }
    }

    private static DorisOptions createDorisOptions() {
        Map<String, String> config = new HashMap<>();
        config.put(DorisSinkConnectorConfig.NAME, "jdbc-tls-it");
        config.put(
                DorisSinkConnectorConfig.DORIS_URLS,
                System.getProperty(DorisCustomerServiceImpl.DORIS_HOST));
        config.put(
                DorisSinkConnectorConfig.DORIS_QUERY_PORT,
                System.getProperty(DorisCustomerServiceImpl.DORIS_QUERY_PORT));
        config.put(
                DorisSinkConnectorConfig.DORIS_HTTP_PORT,
                System.getProperty(DorisCustomerServiceImpl.DORIS_HTTP_PORT, "8030"));
        config.put(
                DorisSinkConnectorConfig.DORIS_USER,
                System.getProperty(DorisCustomerServiceImpl.DORIS_USER));
        config.put(
                DorisSinkConnectorConfig.DORIS_PASSWORD,
                System.getProperty(DorisCustomerServiceImpl.DORIS_PASSWORD));
        config.put(DorisSinkConnectorConfig.DORIS_DATABASE, "information_schema");
        config.put(DorisSinkConnectorConfig.TOPICS_TABLES_MAP, "unused:unused");
        config.put(ConfigCheckUtils.TASK_ID, "0");
        config.put(
                DorisSinkConnectorConfig.DORIS_ENABLE_TLS,
                System.getProperty(DorisCustomerServiceImpl.DORIS_ENABLE_TLS));
        config.put(
                DorisSinkConnectorConfig.DORIS_TLS_CA_CERTIFICATE_PATH,
                System.getProperty(DorisCustomerServiceImpl.DORIS_TLS_CA_CERTIFICATE_PATH, ""));
        config.put(
                DorisSinkConnectorConfig.DORIS_TLS_SKIP_HOSTNAME_VERIFICATION,
                System.getProperty(
                        DorisCustomerServiceImpl.DORIS_TLS_SKIP_HOSTNAME_VERIFICATION, "false"));
        config.put(
                DorisSinkConnectorConfig.DORIS_TLS_EXCLUDED_PROTOCOLS,
                System.getProperty(DorisCustomerServiceImpl.DORIS_TLS_EXCLUDED_PROTOCOLS, ""));
        DorisSinkConnectorConfig.setDefaultValues(config);
        return new DorisOptions(config);
    }
}
