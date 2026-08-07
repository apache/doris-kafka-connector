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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Collection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.apache.doris.kafka.connector.cfg.DorisTlsOptions;
import org.apache.doris.kafka.connector.exception.DorisException;

/** Creates per-client TLS trust material from a PEM CA certificate chain. */
public final class DorisTlsContextFactory {

    private DorisTlsContextFactory() {}

    public static SSLContext createSslContext(DorisTlsOptions options) {
        try {
            KeyStore trustStore = null;
            if (options.isEnabled() && !options.getCaCertificatePath().isEmpty()) {
                trustStore = createTrustStore(options.getCaCertificatePath());
            }
            TrustManagerFactory trustManagerFactory =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(trustStore);
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, trustManagerFactory.getTrustManagers(), null);
            return sslContext;
        } catch (GeneralSecurityException e) {
            throw new DorisException("Unable to initialize the Doris TLS context", e);
        }
    }

    public static KeyStore createTrustStore(String caCertificatePath) {
        try (InputStream input = openCaCertificate(caCertificatePath)) {
            Collection<? extends Certificate> certificates =
                    CertificateFactory.getInstance("X.509").generateCertificates(input);
            if (certificates.isEmpty()) {
                throw new GeneralSecurityException("The CA certificate file is empty");
            }
            KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
            trustStore.load(null, null);
            int index = 0;
            for (Certificate certificate : certificates) {
                trustStore.setCertificateEntry("doris-ca-" + index++, certificate);
            }
            return trustStore;
        } catch (IOException | GeneralSecurityException e) {
            throw caCertificateException(caCertificatePath, e);
        }
    }

    private static InputStream openCaCertificate(String caCertificatePath) {
        Path path = Paths.get(caCertificatePath).toAbsolutePath();
        try {
            return Files.newInputStream(path);
        } catch (IOException e) {
            throw caCertificateException(caCertificatePath, e);
        }
    }

    private static DorisException caCertificateException(
            String caCertificatePath, Exception cause) {
        String absolutePath = Paths.get(caCertificatePath).toAbsolutePath().toString();
        return new DorisException(
                String.format(
                        "Unable to load Doris TLS CA certificate from '%s' (absolute path '%s')",
                        caCertificatePath, absolutePath),
                cause);
    }
}
