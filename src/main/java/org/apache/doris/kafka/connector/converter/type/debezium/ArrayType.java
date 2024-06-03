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
