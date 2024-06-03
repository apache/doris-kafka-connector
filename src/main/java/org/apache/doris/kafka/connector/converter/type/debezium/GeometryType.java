package org.apache.doris.kafka.connector.converter.type.debezium;

import io.debezium.data.geometry.Geometry;
import org.apache.doris.kafka.connector.converter.type.AbstractGeometryType;
import org.apache.doris.kafka.connector.converter.type.util.GeoUtils;
import org.apache.kafka.connect.data.Struct;

public class GeometryType extends AbstractGeometryType {
    public static final GeometryType INSTANCE = new GeometryType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[] {Geometry.LOGICAL_NAME};
    }

    @Override
    public Object getValue(Object sourceValue) {
        if (sourceValue == null) {
            return null;
        }

        if (sourceValue instanceof Struct) {
            return GeoUtils.handleGeoStructData(sourceValue);
        }

        return sourceValue.toString();
    }
}
