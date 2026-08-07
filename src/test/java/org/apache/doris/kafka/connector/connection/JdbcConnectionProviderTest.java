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

package org.apache.doris.kafka.connector.connection;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.doris.kafka.connector.cfg.DorisOptions;
import org.apache.doris.kafka.connector.cfg.DorisTlsOptions;
import org.apache.doris.kafka.connector.testutil.DorisOptionsTestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;

public class JdbcConnectionProviderTest {

    @Test
    public void testTlsRequiresModernMysqlDriver() throws Exception {
        DorisOptions options = DorisOptionsTestUtils.tlsOptions("localhost", 8030);
        List<String> loadedDrivers = new ArrayList<>();
        JdbcConnectionProvider provider =
                new JdbcConnectionProvider(options) {
                    @Override
                    protected void loadDriverClass(String driverName)
                            throws ClassNotFoundException {
                        loadedDrivers.add(driverName);
                        if (JdbcConnectionProvider.CJ_DRIVER_NAME.equals(driverName)) {
                            throw new ClassNotFoundException(driverName);
                        }
                    }
                };

        try (MockedStatic<DriverManager> driverManager = mockStatic(DriverManager.class)) {
            driverManager
                    .when(() -> DriverManager.getConnection(anyString(), any(Properties.class)))
                    .thenReturn(mock(Connection.class));
            try {
                provider.getOrEstablishConnection();
                Assert.fail("TLS must not fall back to the legacy MySQL driver");
            } catch (ClassNotFoundException expected) {
                Assert.assertTrue(
                        expected.getMessage().contains(JdbcConnectionProvider.CJ_DRIVER_NAME));
            }
        }

        Assert.assertEquals(1, loadedDrivers.size());
        Assert.assertEquals(JdbcConnectionProvider.CJ_DRIVER_NAME, loadedDrivers.get(0));
    }

    @Test
    public void testPlaintextModeKeepsLegacyDriverFallback() throws Exception {
        DorisOptions options = mock(DorisOptions.class);
        when(options.getTlsOptions()).thenReturn(DorisTlsOptions.disabled());
        when(options.getQueryUrl()).thenReturn("localhost:9030");
        List<String> loadedDrivers = new ArrayList<>();
        JdbcConnectionProvider provider =
                new JdbcConnectionProvider(options) {
                    @Override
                    protected void loadDriverClass(String driverName)
                            throws ClassNotFoundException {
                        loadedDrivers.add(driverName);
                        if (CJ_DRIVER_NAME.equals(driverName)) {
                            throw new ClassNotFoundException(driverName);
                        }
                    }
                };
        Connection connection = mock(Connection.class);

        try (MockedStatic<DriverManager> driverManager = mockStatic(DriverManager.class)) {
            driverManager
                    .when(() -> DriverManager.getConnection(anyString(), any(Properties.class)))
                    .thenReturn(connection);
            Assert.assertSame(connection, provider.getOrEstablishConnection());
        } finally {
            provider.closeConnection();
        }

        Assert.assertEquals(2, loadedDrivers.size());
        Assert.assertEquals(JdbcConnectionProvider.CJ_DRIVER_NAME, loadedDrivers.get(0));
        Assert.assertEquals(JdbcConnectionProvider.DRIVER_NAME, loadedDrivers.get(1));
    }

    @Test
    public void testConnectionUsesTlsPropertiesAndCloseCleansResources() throws Exception {
        DorisOptions options = DorisOptionsTestUtils.tlsOptions("localhost", 8030);
        Connection connection = mock(Connection.class);
        AtomicReference<Properties> actualProperties = new AtomicReference<>();

        try (MockedStatic<DriverManager> driverManager = mockStatic(DriverManager.class)) {
            driverManager
                    .when(() -> DriverManager.getConnection(anyString(), any(Properties.class)))
                    .thenAnswer(
                            invocation -> {
                                actualProperties.set(invocation.getArgument(1));
                                return connection;
                            });

            JdbcConnectionProvider provider = new JdbcConnectionProvider(options);
            provider.getOrEstablishConnection();

            Assert.assertEquals("VERIFY_IDENTITY", actualProperties.get().getProperty("sslMode"));
            Path trustStorePath =
                    Paths.get(
                            URI.create(
                                    actualProperties
                                            .get()
                                            .getProperty("trustCertificateKeyStoreUrl")));
            Assert.assertTrue(Files.exists(trustStorePath));

            provider.closeConnection();

            verify(connection).close();
            Assert.assertFalse(Files.exists(trustStorePath));
        }
    }

    @Test
    public void testRuntimeConnectionFailureCleansTlsResources() throws Exception {
        DorisOptions options = DorisOptionsTestUtils.tlsOptions("localhost", 8030);
        AtomicReference<Path> trustStorePath = new AtomicReference<>();

        try (MockedStatic<DriverManager> driverManager = mockStatic(DriverManager.class)) {
            driverManager
                    .when(() -> DriverManager.getConnection(anyString(), any(Properties.class)))
                    .thenAnswer(
                            invocation -> {
                                Properties properties = invocation.getArgument(1);
                                trustStorePath.set(
                                        Paths.get(
                                                URI.create(
                                                        properties.getProperty(
                                                                "trustCertificateKeyStoreUrl"))));
                                throw new IllegalStateException("connection failed");
                            });

            JdbcConnectionProvider provider = new JdbcConnectionProvider(options);
            try {
                provider.getOrEstablishConnection();
                Assert.fail("Expected connection failure");
            } catch (IllegalStateException expected) {
                Assert.assertEquals("connection failed", expected.getMessage());
            }
        }

        Assert.assertNotNull(trustStorePath.get());
        Assert.assertFalse(Files.exists(trustStorePath.get()));
    }
}
