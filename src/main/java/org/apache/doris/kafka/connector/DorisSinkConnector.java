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

package org.apache.doris.kafka.connector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.doris.kafka.connector.cfg.DorisSinkConnectorConfig;
import org.apache.doris.kafka.connector.utils.ConfigCheckUtils;
import org.apache.doris.kafka.connector.utils.Version;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** DorisSinkConnector implements SinkConnector for Kafka Connect framework. */
public class DorisSinkConnector extends SinkConnector {
    private static final Logger LOG = LoggerFactory.getLogger(DorisSinkConnector.class);

    private Map<String, String> config;

    /** No-Arg constructor. Required by Kafka Connect framework */
    public DorisSinkConnector() {}

    /**
     * start method will only be called on a clean connector, i.e. it has either just been
     * instantiated and initialized or stop () has been invoked. loads configuration and validates.
     *
     * @param parsedConfig has the configuration settings
     */
    @Override
    public void start(final Map<String, String> parsedConfig) {
        LOG.info("doris sink connector start");
        config = DorisSinkConnectorConfig.convertToLowercase(parsedConfig);
        DorisSinkConnectorConfig.setDefaultValues(config);
        ConfigCheckUtils.validateConfig(config);
    }

    /** stop DorisSinkConnector */
    @Override
    public void stop() {
        LOG.info("doris sink connector stop");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return DorisSinkTask.class;
    }

    /**
     * taskConfigs method returns a set of configurations for SinkTasks based on the current
     * configuration, producing at most 'maxTasks' configurations
     *
     * @param maxTasks maximum number of SinkTasks for this instance of DorisSinkConnector
     * @return a list containing 'maxTasks' copies of the configuration
     */
    @Override
    public List<Map<String, String>> taskConfigs(final int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> conf = new HashMap<>(config);
            conf.put(ConfigCheckUtils.TASK_ID, i + "");
            taskConfigs.add(conf);
        }
        return taskConfigs;
    }

    @Override
    public ConfigDef config() {
        return DorisSinkConnectorConfig.newConfigDef();
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        LOG.info("start validate connector config");
        return super.validate(connectorConfigs);
    }

    @Override
    public String version() {
        return Version.getVersion();
    }
}
