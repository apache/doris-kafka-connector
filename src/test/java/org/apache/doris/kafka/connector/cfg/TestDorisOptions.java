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

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestDorisOptions {

    private DorisOptions dorisOptions;
    private Properties props;

    @Before
    public void init() throws IOException {
        InputStream stream =
                this.getClass()
                        .getClassLoader()
                        .getResourceAsStream("doris-connector-sink.properties");
        props = new Properties();
        props.load(stream);
        props.put("task_id", "1");
        DorisSinkConnectorConfig.setDefaultValues((Map) props);
    }

    @Test
    public void testGetQueryUrls() {
        props.put("doris.urls", "10.20.30.1, 10.20.30.2,10.20.30.3");
        dorisOptions = new DorisOptions((Map) props);
        List<String> queryUrls = dorisOptions.getQueryUrls();
        Set<String> result =
                new HashSet<>(
                        Arrays.asList("10.20.30.1:9030", "10.20.30.2:9030", "10.20.30.3:9030"));

        boolean flag = true;
        for (String queryUrl : queryUrls) {
            if (!result.contains(queryUrl)) {
                flag = false;
                break;
            }
        }
        Assert.assertTrue(flag);

        String queryUrl = dorisOptions.getQueryUrl();
        Assert.assertTrue(result.contains(queryUrl));
    }

    @Test
    public void testDefaultCompressType() {
        props.put("doris.urls", "10.20.30.1");
        dorisOptions = new DorisOptions((Map) props);
        Properties streamLoadProp = dorisOptions.getStreamLoadProp();
        Assert.assertEquals("gz", streamLoadProp.getProperty("compress_type"));
    }

    @Test
    public void testOverrideCompressType() {
        props.put("doris.urls", "10.20.30.1");
        props.put("sink.properties.compress_type", "lz4");
        dorisOptions = new DorisOptions((Map) props);
        Properties streamLoadProp = dorisOptions.getStreamLoadProp();
        Assert.assertEquals("lz4", streamLoadProp.getProperty("compress_type"));
    }

    @Test
    public void testDisableCompressType() {
        props.put("doris.urls", "10.20.30.1");
        props.put("sink.properties.compress_type", "");
        dorisOptions = new DorisOptions((Map) props);
        Properties streamLoadProp = dorisOptions.getStreamLoadProp();
        Assert.assertEquals("", streamLoadProp.getProperty("compress_type"));
    }

    @Test
    public void testGetHttpUrls() {
        props.put("doris.urls", "10.20.30.1,10.20.30.2, 10.20.30.3");
        dorisOptions = new DorisOptions((Map) props);
        List<String> httpUrls = dorisOptions.getHttpUrls();
        Set<String> result =
                new HashSet<>(
                        Arrays.asList("10.20.30.1:8030", "10.20.30.2:8030", "10.20.30.3:8030"));

        boolean flag = true;
        for (String queryUrl : httpUrls) {
            if (!result.contains(queryUrl)) {
                flag = false;
                break;
            }
        }
        Assert.assertTrue(flag);

        String httpUrl = dorisOptions.getHttpUrl();
        Assert.assertTrue(result.contains(httpUrl));
    }

    @Test
    public void testTlsOptions() {
        props.put(DorisSinkConnectorConfig.DORIS_ENABLE_TLS, "true");
        props.put(DorisSinkConnectorConfig.DORIS_TLS_CA_CERTIFICATE_PATH, "certs/ca.pem");
        props.put(DorisSinkConnectorConfig.DORIS_TLS_SKIP_HOSTNAME_VERIFICATION, "true");
        props.put(DorisSinkConnectorConfig.DORIS_TLS_EXCLUDED_PROTOCOLS, "mysql");

        dorisOptions = new DorisOptions((Map) props);
        DorisTlsOptions tlsOptions = dorisOptions.getTlsOptions();

        Assert.assertTrue(tlsOptions.isEnabled());
        Assert.assertTrue(tlsOptions.isEnabledFor(DorisTlsOptions.Protocol.HTTP));
        Assert.assertFalse(tlsOptions.isEnabledFor(DorisTlsOptions.Protocol.MYSQL));
        Assert.assertEquals("certs/ca.pem", tlsOptions.getCaCertificatePath());
        Assert.assertTrue(tlsOptions.isSkipHostnameVerification());
    }

    @Test
    public void testS3TvfOptionsAndSessionVariables() {
        Map<String, String> config = TestDorisSinkConnectorConfig.getS3TvfConfig();
        config.put("task_id", "2");
        config.put("sink.properties.enable_unique_key_partial_update", "true");
        config.put("sink.properties.partial_update_new_key_behavior", "ERROR");
        config.put("sink.properties.format", "json");

        DorisOptions options = new DorisOptions(config);

        Assert.assertEquals(2, options.getTaskId());
        Assert.assertEquals("staging", options.getS3TvfOptions().getBucket());
        Assert.assertEquals(Arrays.asList("id", "name"), options.getTvfColumns());
        Assert.assertEquals(
                "true", options.getSessionVariables().get("enable_unique_key_partial_update"));
        Assert.assertEquals(
                "ERROR", options.getSessionVariables().get("partial_update_new_key_behavior"));
        Assert.assertFalse(options.getSessionVariables().containsKey("format"));
        Assert.assertFalse(options.getSessionVariables().containsKey("compress_type"));
        Assert.assertTrue(options.isGzipCompressionEnabled());

        config.put("sink.properties.compress_type", "");
        Assert.assertFalse(new DorisOptions(config).isGzipCompressionEnabled());

        config.remove(DorisSinkConnectorConfig.SINK_S3_ACCESS_KEY);
        config.remove(DorisSinkConnectorConfig.SINK_S3_SECRET_KEY);
        config.put(
                DorisSinkConnectorConfig.SINK_S3_ROLE_ARN, "arn:aws:iam::123456789012:role/doris");
        config.put(DorisSinkConnectorConfig.SINK_S3_EXTERNAL_ID, "external-id");
        S3TvfOptions roleOptions = new DorisOptions(config).getS3TvfOptions();
        Assert.assertEquals("arn:aws:iam::123456789012:role/doris", roleOptions.getRoleArn());
        Assert.assertEquals("external-id", roleOptions.getExternalId());
    }
}
