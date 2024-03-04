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

package org.apache.doris.kafka.connector.utils;

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class ConfigCheckUtilsTest {

    @Test
    public void testIsValidDorisApplicationName() {
        assert ConfigCheckUtils.isValidDorisApplicationName("-_aA1");
        assert ConfigCheckUtils.isValidDorisApplicationName("aA_1-");
        assert !ConfigCheckUtils.isValidDorisApplicationName("_1.a$");
        assert !ConfigCheckUtils.isValidDorisApplicationName("(1.f$-_");
    }

    @Test
    public void testTableName() {
        Map<String, String> topic2table =
                ConfigCheckUtils.parseTopicToTableMap("ab@cd:abcd, 1234:_1234");

        assert ConfigCheckUtils.tableName("ab@cd", topic2table).equals("abcd");
        assert ConfigCheckUtils.tableName("1234", topic2table).equals("_1234");
    }

    @Test
    public void testTableNameMapEmpty() {
        Map<String, String> topic2table = new HashMap<>();

        assert ConfigCheckUtils.tableName("name.db.tbl", topic2table).equals("tbl");
        assert ConfigCheckUtils.tableName("table123", topic2table).equals("table123");
    }
}
