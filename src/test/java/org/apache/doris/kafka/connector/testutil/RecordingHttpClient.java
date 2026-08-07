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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Queue;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.ProtocolVersion;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicStatusLine;
import org.apache.http.params.HttpParams;
import org.apache.http.protocol.HttpContext;
import org.mockito.Mockito;

/** HTTP client test double that records real requests emitted by production code. */
public final class RecordingHttpClient extends CloseableHttpClient {

    private final Queue<CloseableHttpResponse> responses = new ArrayDeque<>();
    private String lastRequestUri;
    private String lastRequestMethod;
    private boolean closed;

    public void addResponse(int statusCode, String body) {
        addResponse(statusCode, body, null);
    }

    public void addResponse(int statusCode, String body, String location) {
        CloseableHttpResponse response = Mockito.mock(CloseableHttpResponse.class);
        Mockito.when(response.getStatusLine())
                .thenReturn(new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), statusCode, ""));
        if (body != null) {
            HttpEntity entity = new StringEntity(body, StandardCharsets.UTF_8);
            Mockito.when(response.getEntity()).thenReturn(entity);
        }
        if (location != null) {
            Mockito.when(response.getFirstHeader("location"))
                    .thenReturn(new BasicHeader("location", location));
        }
        responses.add(response);
    }

    public String getLastRequestUri() {
        return lastRequestUri;
    }

    public String getLastRequestMethod() {
        return lastRequestMethod;
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    @Deprecated
    public HttpParams getParams() {
        return null;
    }

    @Override
    @Deprecated
    public ClientConnectionManager getConnectionManager() {
        return null;
    }

    @Override
    protected CloseableHttpResponse doExecute(
            HttpHost target, HttpRequest request, HttpContext context) throws IOException {
        lastRequestUri = request.getRequestLine().getUri();
        lastRequestMethod = request.getRequestLine().getMethod();
        CloseableHttpResponse response = responses.poll();
        if (response == null) {
            throw new IOException("No response configured");
        }
        return response;
    }

    @Override
    public void close() {
        closed = true;
    }
}
