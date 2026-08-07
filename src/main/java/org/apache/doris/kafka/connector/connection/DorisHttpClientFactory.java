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
import java.net.HttpURLConnection;
import java.net.URL;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import org.apache.doris.kafka.connector.cfg.DorisTlsOptions;
import org.apache.doris.kafka.connector.utils.DorisRedirectStrategy;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;

/** Applies Doris TLS settings to HTTP clients without changing JVM global state. */
public final class DorisHttpClientFactory {

    private DorisHttpClientFactory() {}

    public static HttpClientBuilder configure(HttpClientBuilder builder, DorisTlsOptions options) {
        if (!options.isEnabledFor(DorisTlsOptions.Protocol.HTTP)) {
            return builder;
        }
        SSLContext sslContext = DorisTlsContextFactory.createSslContext(options);
        return builder.setSSLContext(sslContext)
                .setSSLHostnameVerifier(
                        options.isSkipHostnameVerification()
                                ? NoopHostnameVerifier.INSTANCE
                                : SSLConnectionSocketFactory.getDefaultHostnameVerifier())
                .setRedirectStrategy(new DorisRedirectStrategy(options));
    }

    public static HttpClientBuilder configureSystemTrust(HttpClientBuilder builder) {
        return builder.setSSLContext(
                        DorisTlsContextFactory.createSslContext(DorisTlsOptions.disabled()))
                .setSSLHostnameVerifier(SSLConnectionSocketFactory.getDefaultHostnameVerifier());
    }

    public static CloseableHttpClient create(DorisTlsOptions options) {
        return configure(HttpClients.custom(), options).build();
    }

    public static HttpURLConnection openConnection(URL url, DorisTlsOptions options)
            throws IOException {
        if (options.isEnabledFor(DorisTlsOptions.Protocol.HTTP)
                && !"https".equalsIgnoreCase(url.getProtocol())) {
            throw new IOException("Doris HTTP TLS requires HTTPS URL: " + url);
        }
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        if (!(connection instanceof HttpsURLConnection)
                || !options.isEnabledFor(DorisTlsOptions.Protocol.HTTP)) {
            return connection;
        }

        HttpsURLConnection httpsConnection = (HttpsURLConnection) connection;
        httpsConnection.setSSLSocketFactory(
                DorisTlsContextFactory.createSslContext(options).getSocketFactory());
        httpsConnection.setHostnameVerifier(
                options.isSkipHostnameVerification()
                        ? (hostname, session) -> true
                        : SSLConnectionSocketFactory.getDefaultHostnameVerifier());
        return httpsConnection;
    }
}
