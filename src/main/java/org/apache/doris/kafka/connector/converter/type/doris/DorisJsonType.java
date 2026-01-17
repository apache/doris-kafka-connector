package org.apache.doris.kafka.connector.converter.type.doris;

import io.debezium.data.Json;
import org.apache.doris.kafka.connector.converter.type.AbstractType;
import org.apache.kafka.connect.data.Schema;

public class DorisJsonType extends AbstractType {

    public static final DorisJsonType INSTANCE = new DorisJsonType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[] {Json.LOGICAL_NAME};
    }

    @Override
    public String getTypeName(Schema schema) {
        return DorisType.JSON;
    }
}
