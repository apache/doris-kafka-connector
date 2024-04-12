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
package org.apache.doris.kafka.connector.converter.type.connect;

import org.apache.doris.kafka.connector.converter.type.AbstractDateType;
import org.apache.doris.kafka.connector.converter.utils.DateTimeUtils;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.errors.ConnectException;

public class ConnectDateType extends AbstractDateType {

    public static final ConnectDateType INSTANCE = new ConnectDateType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[] {Date.LOGICAL_NAME};
    }

    @Override
    public Object getValue(Object sourceValue) {
        if (sourceValue == null) {
            return null;
        }
        if (sourceValue instanceof java.util.Date) {
            return DateTimeUtils.toLocalDateFromDate((java.util.Date) sourceValue);
        }
        throw new ConnectException(
                String.format(
                        "Unexpected %s value '%s' with type '%s'",
                        getClass().getSimpleName(), sourceValue, sourceValue.getClass().getName()));
    }
}
