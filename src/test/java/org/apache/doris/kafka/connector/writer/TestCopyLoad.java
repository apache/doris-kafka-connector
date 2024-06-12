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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import org.apache.doris.kafka.connector.cfg.DorisOptions;
import org.apache.doris.kafka.connector.cfg.DorisSinkConnectorConfig;
import org.apache.doris.kafka.connector.writer.load.CopyLoad;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestCopyLoad {

    private DorisOptions options;

    @Before
    public void init() throws IOException {
        InputStream stream =
                this.getClass()
                        .getClassLoader()
                        .getResourceAsStream("doris-connector-sink.properties");
        Properties props = new Properties();
        props.load(stream);
        props.put("task_id", "1");
        DorisSinkConnectorConfig.setDefaultValues((Map) props);
        options = new DorisOptions((Map) props);
    }

    @Test
    public void executeCopy() throws IOException {
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        String response =
                "{\"msg\":\"success\",\"code\":0,\"data\":{\"result\":{\"msg\":\"\",\"loadedRows\":\"1248\""
                        + ",\"id\":\"7f590a04139949f6-aa8b38048974da1f\",\"state\":\"FINISHED\",\"type\":\"\",\"filterRows\":\"0\",\"unselectRows\":\"0\",\"url\":null},\"time\":5020,\"type\":\"result_set\"},\"count\":0}";
        CloseableHttpResponse copyQueryResponse = HttpTestUtil.getResponse(response, true, false);
        when(httpClient.execute(any())).thenReturn(copyQueryResponse);
        CopyLoad copyLoad = new CopyLoad(options.getDatabase(), "test_kafka", options, httpClient);
        boolean file = copyLoad.executeCopy(Arrays.asList("file", "file2"));
        Assert.assertEquals(true, file);
    }
}
