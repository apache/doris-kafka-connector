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

import java.util.Arrays;
import org.junit.Assert;
import org.junit.Test;

public class TestCopySQL {

    @Test
    public void testCopySQL() {
        CopySQLBuilder builder =
                new CopySQLBuilder("db", "tbl", Arrays.asList("fileName", "fileName2"), false);
        String copySQL = builder.buildCopySQL();
        String except =
                "COPY INTO db.tbl FROM @~('{fileName,fileName2}') PROPERTIES ('copy.async'='false','file.type'='json','file.strip_outer_array'='false','copy.use_delete_sign'='false')";
        Assert.assertEquals(copySQL, except);
    }
}
