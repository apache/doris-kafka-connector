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

package org.apache.doris.kafka.connector.writer.load;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import org.apache.http.HttpEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DataLoad implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(DataLoad.class);
    protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    // hard cap on how much of an error-response body is read into memory and reported;
    // error pages of proxies/FE can be arbitrarily large
    private static final int RESPONSE_SNIPPET_MAX_LEN = 512;
    protected String database;
    protected String table;
    protected String user;
    protected String password;

    /**
     * Read at most {@value #RESPONSE_SNIPPET_MAX_LEN} (plus one, to detect truncation) bytes of an
     * error-response body into memory and return them as a single-line snippet for diagnostics. The
     * body is never read fully, so a hostile or misbehaving server or proxy cannot force an
     * unbounded memory allocation on the error path. Returns {@code <no body>} when the entity is
     * null and {@code <unreadable body>} when the body cannot be read.
     *
     * <p>Note: this only bounds the length; it does not redact sensitive data that the server or an
     * intermediary may include in an error page.
     */
    protected static String errorBodySnippet(HttpEntity entity) {
        if (entity == null) {
            return "<no body>";
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (InputStream in = entity.getContent()) {
            if (in == null) {
                return "<no body>";
            }
            byte[] chunk = new byte[RESPONSE_SNIPPET_MAX_LEN + 1];
            int n;
            while (out.size() <= RESPONSE_SNIPPET_MAX_LEN
                    && (n = in.read(chunk, 0, RESPONSE_SNIPPET_MAX_LEN + 1 - out.size())) != -1) {
                out.write(chunk, 0, n);
            }
        } catch (IOException | RuntimeException e) {
            return "<unreadable body>";
        }
        if (out.size() == 0) {
            return "<empty body>";
        }
        boolean truncated = out.size() > RESPONSE_SNIPPET_MAX_LEN;
        String body =
                new String(
                        out.toByteArray(),
                        0,
                        Math.min(out.size(), RESPONSE_SNIPPET_MAX_LEN),
                        StandardCharsets.UTF_8);
        String compact = body.replaceAll("\\s+", " ").trim();
        if (compact.isEmpty()) {
            return "<empty body>";
        }
        if (truncated || compact.length() > RESPONSE_SNIPPET_MAX_LEN) {
            return compact.substring(0, RESPONSE_SNIPPET_MAX_LEN) + "...";
        }
        return compact;
    }
}
