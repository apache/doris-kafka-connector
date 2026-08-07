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

package org.apache.doris.kafka.connector.cfg;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.EnumSet;
import org.junit.Assert;
import org.junit.Test;

public class DorisTlsOptionsTest {

    @Test
    public void testDisabledDefaults() {
        DorisTlsOptions options = DorisTlsOptions.disabled();

        Assert.assertFalse(options.isEnabled());
        Assert.assertFalse(options.isEnabledFor(DorisTlsOptions.Protocol.HTTP));
        Assert.assertFalse(options.isEnabledFor(DorisTlsOptions.Protocol.MYSQL));
        Assert.assertEquals("", options.getCaCertificatePath());
        Assert.assertFalse(options.isSkipHostnameVerification());
        Assert.assertTrue(options.getExcludedProtocols().isEmpty());
    }

    @Test
    public void testExcludedProtocolsAreNormalized() {
        DorisTlsOptions options =
                DorisTlsOptions.builder()
                        .setEnabled(true)
                        .setExcludedProtocols(" HTTP, mysql,HTTP ")
                        .build();

        Assert.assertFalse(options.isEnabledFor(DorisTlsOptions.Protocol.HTTP));
        Assert.assertFalse(options.isEnabledFor(DorisTlsOptions.Protocol.MYSQL));
        Assert.assertEquals(
                EnumSet.of(DorisTlsOptions.Protocol.HTTP, DorisTlsOptions.Protocol.MYSQL),
                options.getExcludedProtocols());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUnknownExcludedProtocolIsRejected() {
        DorisTlsOptions.builder().setExcludedProtocols("thrift").build();
    }

    @Test
    public void testSerializationRoundTrip() throws Exception {
        DorisTlsOptions options =
                DorisTlsOptions.builder()
                        .setEnabled(true)
                        .setCaCertificatePath("certs/ca-chain.pem")
                        .setSkipHostnameVerification(true)
                        .setExcludedProtocols("mysql")
                        .build();

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(options);
        }

        DorisTlsOptions restored;
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            restored = (DorisTlsOptions) input.readObject();
        }

        Assert.assertEquals(options, restored);
    }
}
