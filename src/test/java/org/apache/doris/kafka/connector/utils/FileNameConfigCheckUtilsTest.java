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

import org.junit.Assert;
import org.junit.Test;

public class FileNameConfigCheckUtilsTest {

    String name = "doris-connector-test";
    String topic = "avro-complex10";
    public String fileName1 =
            "doris-connector-test__KC_avro-complex10__KC_0__KC_411__KC_1706149860394";
    public String fileName2 =
            "doris-connector-test__KC_avro-complex10__KC_2__KC_348__KC_1706149860395";
    public String fileName3 =
            "doris-connector-test__KC_avro-complex10__KC_5__KC_311__KC_1706149860394";

    @Test
    public void verifyFileNameTest() {
        Assert.assertTrue(FileNameUtils.verifyFileName(name, topic, 0, fileName1));
        Assert.assertFalse(FileNameUtils.verifyFileName(name, topic, 1, fileName1));

        Assert.assertTrue(FileNameUtils.verifyFileName(name, topic, 2, fileName2));
        Assert.assertFalse(FileNameUtils.verifyFileName(name, topic, 12, fileName2));

        Assert.assertFalse(FileNameUtils.verifyFileName(name, topic, 15, fileName3));
        Assert.assertTrue(FileNameUtils.verifyFileName(name, topic, 5, fileName3));
    }
}
