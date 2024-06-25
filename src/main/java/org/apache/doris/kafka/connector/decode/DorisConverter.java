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

package org.apache.doris.kafka.connector.decode;

import java.util.Map;
import org.apache.doris.kafka.connector.exception.DataDecodeException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.storage.Converter;

public abstract class DorisConverter implements Converter {

    /** unused */
    @Override
    public void configure(final Map<String, ?> map, final boolean b) {
        // not necessary
    }

    /** doesn't support data source connector */
    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        throw new DataDecodeException("DorisConverter doesn't support data source connector yet.");
    }
}
