package org.apache.doris.kafka.connector.converter.type.debezium;

import io.debezium.data.geometry.Point;
import org.apache.doris.kafka.connector.converter.type.AbstractGeometryType;
import org.apache.doris.kafka.connector.converter.type.util.GeoUtils;
import org.apache.kafka.connect.data.Struct;

public class PointType extends AbstractGeometryType {
    public static final PointType INSTANCE = new PointType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[] {Point.LOGICAL_NAME};
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
