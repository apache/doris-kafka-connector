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
}
