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

import java.util.HashMap;
import java.util.Map;
import org.apache.doris.kafka.connector.cfg.DorisOptions;
import org.apache.doris.kafka.connector.cfg.DorisSinkConnectorConfig;
import org.apache.doris.kafka.connector.utils.ConfigCheckUtils;

public final class DorisOptionsTestUtils {

    private DorisOptionsTestUtils() {}

    public static DorisOptions tlsOptions(String host, int httpPort) throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put(DorisSinkConnectorConfig.NAME, "tls-test");
        config.put(DorisSinkConnectorConfig.DORIS_URLS, host);
        config.put(DorisSinkConnectorConfig.DORIS_QUERY_PORT, "9030");
        config.put(DorisSinkConnectorConfig.DORIS_HTTP_PORT, String.valueOf(httpPort));
        config.put(DorisSinkConnectorConfig.DORIS_USER, "root");
        config.put(DorisSinkConnectorConfig.DORIS_PASSWORD, "password");
        config.put(DorisSinkConnectorConfig.DORIS_DATABASE, "db");
        config.put(DorisSinkConnectorConfig.TOPICS_TABLES_MAP, "topic:table");
        config.put(DorisSinkConnectorConfig.AUTO_REDIRECT, "false");
        config.put(DorisSinkConnectorConfig.DORIS_ENABLE_TLS, "true");
        config.put(
                DorisSinkConnectorConfig.DORIS_TLS_CA_CERTIFICATE_PATH,
                HttpsTestServer.resourcePath("/tls/ca.pem"));
        config.put(ConfigCheckUtils.TASK_ID, "0");
        DorisSinkConnectorConfig.setDefaultValues(config);
        return new DorisOptions(config);
    }
}
