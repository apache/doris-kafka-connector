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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Test;

public class TestRecordBuffer {

    private RecordBuffer buffer = new RecordBuffer();

    @Test
    public void insert() {
        long offset = 1l;
        buffer.insert(newSinkRecord("doris", offset).value().toString());
        String data = buffer.getData();
        Assert.assertEquals(data, "doris");
        offset++;
        buffer.insert(newSinkRecord("doris", offset).value().toString());
        Assert.assertEquals(2, buffer.getNumOfRecords());
    }

    public static SinkRecord newSinkRecord(Object value, long offset) {
        SinkRecord record =
                new SinkRecord(
                        "topic",
                        0,
                        Schema.OPTIONAL_STRING_SCHEMA,
                        "key",
                        Schema.OPTIONAL_STRING_SCHEMA,
                        value,
                        offset);
        return record;
    }

    public static SinkRecord newSinkRecord(Object value, long offset, Schema valueSchema) {
        SinkRecord record =
                new SinkRecord(
                        "topic",
                        0,
                        Schema.OPTIONAL_STRING_SCHEMA,
                        "key",
                        valueSchema,
                        value,
                        offset);
        return record;
    }
}
