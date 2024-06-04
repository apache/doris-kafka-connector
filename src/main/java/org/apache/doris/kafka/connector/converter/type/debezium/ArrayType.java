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

package org.apache.doris.kafka.connector.converter.type.debezium;

import org.apache.doris.kafka.connector.converter.type.AbstractType;
import org.apache.doris.kafka.connector.converter.type.doris.DorisType;
import org.apache.kafka.connect.data.Schema;

public class ArrayType extends AbstractType {
    public static final ArrayType INSTANCE = new ArrayType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[] {"ARRAY"};
    }

    @Override
    public String getTypeName(Schema schema) {
        return DorisType.STRING;
    }

    @Override
    public Object getValue(Object sourceValue) {
        return sourceValue.toString();
    }
}
