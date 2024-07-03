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

import java.util.Arrays;
import java.util.List;

public enum GroupCommitMode {
    OFF_MODE("off_mode"),
    SYNC_MODE("sync_mode"),
    ASYNC_MODE("async_mode");

    private final String name;

    GroupCommitMode(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static LoadModel of(String name) {
        return LoadModel.valueOf(name.toUpperCase());
    }

    public static List<String> instances() {
        return Arrays.asList(OFF_MODE.name, SYNC_MODE.name, ASYNC_MODE.name);
    }
}
