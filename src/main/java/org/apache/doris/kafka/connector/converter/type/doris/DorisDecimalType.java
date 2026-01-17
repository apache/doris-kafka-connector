package org.apache.doris.kafka.connector.converter.type.doris;

import org.apache.doris.kafka.connector.converter.type.connect.ConnectDecimalType;
import org.apache.kafka.connect.data.Schema;

public class DorisDecimalType extends ConnectDecimalType {

    public static final DorisDecimalType INSTANCE = new DorisDecimalType();

    @Override
    public String getTypeName(Schema schema) {
        int scale = Integer.parseInt(getSchemaParameter(schema, "scale").orElse("0"));
        int precision =
                Integer.parseInt(
                        getSchemaParameter(schema, "connect.decimal.precision").orElse("38"));
        return precision <= 38
                ? String.format("%s(%s,%s)", DorisType.DECIMAL, precision, Math.max(scale, 0))
                : DorisType.STRING;
    }
}
