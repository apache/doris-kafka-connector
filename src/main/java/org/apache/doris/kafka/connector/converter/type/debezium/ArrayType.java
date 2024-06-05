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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.doris.kafka.connector.cfg.DorisOptions;
import org.apache.doris.kafka.connector.converter.RecordTypeRegister;
import org.apache.doris.kafka.connector.converter.type.AbstractType;
import org.apache.doris.kafka.connector.converter.type.Type;
import org.apache.doris.kafka.connector.converter.type.doris.DorisType;
import org.apache.kafka.connect.data.Schema;

public class ArrayType extends AbstractType {
    DorisOptions dorisOptions;
    private static final String ARRAY_TYPE_TEMPLATE = "%s<%s>";
    public static final ArrayType INSTANCE = new ArrayType();

    @Override
    public void configure(DorisOptions dorisOptions) {
        this.dorisOptions = dorisOptions;
    }

    @Override
    public String[] getRegistrationKeys() {
        return new String[] {"ARRAY"};
    }

    @Override
    public String getTypeName(Schema schema) {
        if (schema.valueSchema().isOptional()) {
            Schema valueSchema = schema.valueSchema();
            String type =
                    Objects.nonNull(valueSchema.name())
                            ? valueSchema.name()
                            : valueSchema.type().name();
            Type valueType = new RecordTypeRegister(dorisOptions).getTypeRegistry().get(type);
            if (valueType == null) {
                return DorisType.STRING;
            }
            String typeName = valueType.getTypeName(schema);
            return String.format(ARRAY_TYPE_TEMPLATE, DorisType.ARRAY, typeName);
        }
        return DorisType.STRING;
    }

    @Override
    public Object getValue(Object sourceValue, Schema schema) {

        if (sourceValue == null) {
            return null;
        }
        Schema valueSchema = schema.valueSchema();
        String type =
                Objects.nonNull(valueSchema.name())
                        ? valueSchema.name()
                        : valueSchema.type().name();

        if (sourceValue instanceof ArrayList) {
            List<Object> resultList = new ArrayList<>();
            ArrayList<?> convertedValue = (ArrayList<?>) sourceValue;
            Type valueType = new RecordTypeRegister(dorisOptions).getTypeRegistry().get(type);
            for (Object value : convertedValue) {
                if (valueType == null) {
                    return sourceValue;
                }
                resultList.add(valueType.getValue(value, valueSchema));
            }
            return resultList;
        }

        return sourceValue;
    }
}
