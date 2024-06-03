package org.apache.doris.kafka.connector.converter.type.debezium;

import io.debezium.data.geometry.Geography;
import org.apache.doris.kafka.connector.converter.type.AbstractGeometryType;

public class GeographyType extends AbstractGeometryType {
    public static final GeographyType INSTANCE = new GeographyType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[] {Geography.LOGICAL_NAME};
    }
}
