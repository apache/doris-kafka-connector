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

import java.util.HashMap;
import java.util.Map;
import org.apache.doris.kafka.connector.cfg.DorisTlsOptions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DorisCustomerServiceImplTest {

    private static final String[] PROPERTY_NAMES = {
        DorisCustomerServiceImpl.DORIS_HOST,
        DorisCustomerServiceImpl.DORIS_QUERY_PORT,
        DorisCustomerServiceImpl.DORIS_HTTP_PORT,
        DorisCustomerServiceImpl.DORIS_USER,
        DorisCustomerServiceImpl.DORIS_PASSWORD,
        DorisCustomerServiceImpl.DORIS_ENABLE_TLS,
        DorisCustomerServiceImpl.DORIS_TLS_CA_CERTIFICATE_PATH,
        DorisCustomerServiceImpl.DORIS_TLS_SKIP_HOSTNAME_VERIFICATION,
        DorisCustomerServiceImpl.DORIS_TLS_EXCLUDED_PROTOCOLS
    };

    private final Map<String, String> originalProperties = new HashMap<>();

    @Before
    public void setUp() {
        for (String name : PROPERTY_NAMES) {
            originalProperties.put(name, System.getProperty(name));
        }
        System.setProperty(DorisCustomerServiceImpl.DORIS_HOST, "doris.example.com");
        System.setProperty(DorisCustomerServiceImpl.DORIS_QUERY_PORT, "19030");
        System.setProperty(DorisCustomerServiceImpl.DORIS_HTTP_PORT, "18030");
        System.setProperty(DorisCustomerServiceImpl.DORIS_USER, "test_user");
        System.setProperty(DorisCustomerServiceImpl.DORIS_PASSWORD, "test_password");
        System.setProperty(DorisCustomerServiceImpl.DORIS_ENABLE_TLS, "true");
        System.setProperty(
                DorisCustomerServiceImpl.DORIS_TLS_CA_CERTIFICATE_PATH,
                "src/test/resources/tls/ca.pem");
        System.setProperty(DorisCustomerServiceImpl.DORIS_TLS_SKIP_HOSTNAME_VERIFICATION, "true");
        System.setProperty(DorisCustomerServiceImpl.DORIS_TLS_EXCLUDED_PROTOCOLS, "mysql");
    }

    @After
    public void tearDown() {
        for (Map.Entry<String, String> entry : originalProperties.entrySet()) {
            if (entry.getValue() == null) {
                System.clearProperty(entry.getKey());
            } else {
                System.setProperty(entry.getKey(), entry.getValue());
            }
        }
    }

    @Test
    public void testReadExternalTlsEnvironment() {
        DorisCustomerServiceImpl service = new DorisCustomerServiceImpl();
        service.validateProperties();

        Assert.assertEquals("doris.example.com", service.getInstanceHost());
        Assert.assertEquals(19030, service.getQueryPort());
        Assert.assertEquals(18030, service.getHttpPort());
        Assert.assertEquals("test_user", service.getUsername());
        Assert.assertEquals("test_password", service.getPassword());

        DorisTlsOptions tlsOptions = service.getTlsOptions();
        Assert.assertTrue(tlsOptions.isEnabledFor(DorisTlsOptions.Protocol.HTTP));
        Assert.assertFalse(tlsOptions.isEnabledFor(DorisTlsOptions.Protocol.MYSQL));
        Assert.assertTrue(tlsOptions.isSkipHostnameVerification());
        Assert.assertEquals("src/test/resources/tls/ca.pem", tlsOptions.getCaCertificatePath());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRejectMissingPasswordProperty() {
        System.clearProperty(DorisCustomerServiceImpl.DORIS_PASSWORD);
        new DorisCustomerServiceImpl().validateProperties();
    }
}
