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

package org.apache.doris.kafka.connector.writer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.message.BasicStatusLine;

/** Test Util for Http. */
public class HttpTestUtil {

    public static StatusLine normalLine =
            new BasicStatusLine(new ProtocolVersion("http", 1, 0), 200, "");
    public static StatusLine abnormalLine =
            new BasicStatusLine(new ProtocolVersion("http", 1, 0), 404, "");
    public static StatusLine redirectLine =
            new BasicStatusLine(new ProtocolVersion("http", 1, 0), 307, "");

    public static CloseableHttpResponse getResponse(
            String response, boolean ok, boolean isRedirect) {
        HttpEntityMock httpEntityMock = new HttpEntityMock();
        httpEntityMock.setValue(response);
        CloseableHttpResponse httpResponse = mock(CloseableHttpResponse.class);
        if (isRedirect) {
            when(httpResponse.getStatusLine()).thenReturn(redirectLine);
        } else if (ok) {
            when(httpResponse.getStatusLine()).thenReturn(normalLine);
        } else {
            when(httpResponse.getStatusLine()).thenReturn(abnormalLine);
        }
        when(httpResponse.getEntity()).thenReturn(httpEntityMock);
        return httpResponse;
    }
}
