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

package org.apache.doris.kafka.connector.testutil;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import org.apache.commons.io.IOUtils;

/** Local HTTPS server backed by test-only PEM key material. */
public final class HttpsTestServer implements AutoCloseable {

    private static final char[] KEY_PASSWORD = "changeit".toCharArray();

    private final HttpsServer server;
    private final AtomicReference<String> lastRequestPath = new AtomicReference<>();
    private final AtomicReference<String> lastRequestMethod = new AtomicReference<>();

    public HttpsTestServer() throws Exception {
        this("ok");
    }

    public HttpsTestServer(String responseBody) throws Exception {
        Collection<? extends Certificate> certificates;
        try (InputStream input = resource("/tls/server-chain.pem")) {
            certificates = CertificateFactory.getInstance("X.509").generateCertificates(input);
        }

        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        keyStore.load(null, null);
        keyStore.setKeyEntry(
                "server",
                loadPrivateKey(),
                KEY_PASSWORD,
                certificates.toArray(new Certificate[certificates.size()]));

        KeyManagerFactory keyManagerFactory =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, KEY_PASSWORD);
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagerFactory.getKeyManagers(), null, null);

        server =
                HttpsServer.create(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0), 0);
        server.setHttpsConfigurator(new HttpsConfigurator(sslContext));
        server.createContext("/", exchange -> respondAndRecord(exchange, responseBody));
        server.start();
    }

    public String getUrl(String host) {
        return "https://" + host + ":" + server.getAddress().getPort() + "/";
    }

    public String getEndpoint(String host) {
        return host + ":" + server.getAddress().getPort();
    }

    public int getPort() {
        return server.getAddress().getPort();
    }

    public String getLastRequestPath() {
        return lastRequestPath.get();
    }

    public String getLastRequestMethod() {
        return lastRequestMethod.get();
    }

    public void redirectTo(String location) {
        server.removeContext("/");
        server.createContext(
                "/",
                exchange -> {
                    record(exchange);
                    exchange.getResponseHeaders().add("Location", location);
                    exchange.sendResponseHeaders(307, -1);
                    exchange.close();
                });
    }

    public void redirectPathTo(String path, String location) {
        server.removeContext("/");
        server.createContext(
                "/",
                exchange -> {
                    record(exchange);
                    if (path.equals(exchange.getRequestURI().getPath())) {
                        exchange.getResponseHeaders().add("Location", location);
                        exchange.sendResponseHeaders(307, -1);
                        exchange.close();
                    } else {
                        respond(exchange, "ok");
                    }
                });
    }

    public static String resourcePath(String resource) throws URISyntaxException {
        Path path = Paths.get(HttpsTestServer.class.getResource(resource).toURI());
        return path.toString();
    }

    @Override
    public void close() {
        server.stop(0);
    }

    private static PrivateKey loadPrivateKey() throws Exception {
        String pem;
        try (InputStream input = resource("/tls/server-key.pem")) {
            pem = new String(IOUtils.toByteArray(input), StandardCharsets.UTF_8);
        }
        String encoded =
                pem.replace("-----BEGIN PRIVATE KEY-----", "")
                        .replace("-----END PRIVATE KEY-----", "")
                        .replaceAll("\\s", "");
        return KeyFactory.getInstance("RSA")
                .generatePrivate(new PKCS8EncodedKeySpec(Base64.getDecoder().decode(encoded)));
    }

    private static InputStream resource(String path) {
        InputStream input = HttpsTestServer.class.getResourceAsStream(path);
        if (input == null) {
            throw new IllegalStateException("Missing TLS test resource: " + path);
        }
        return input;
    }

    private static void respond(HttpExchange exchange, String responseBody) throws IOException {
        byte[] response = responseBody.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, response.length);
        exchange.getResponseBody().write(response);
        exchange.close();
    }

    private void respondAndRecord(HttpExchange exchange, String responseBody) throws IOException {
        record(exchange);
        respond(exchange, responseBody);
    }

    private void record(HttpExchange exchange) {
        lastRequestPath.set(exchange.getRequestURI().toString());
        lastRequestMethod.set(exchange.getRequestMethod());
    }
}
