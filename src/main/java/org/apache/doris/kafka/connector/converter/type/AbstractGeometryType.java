package org.apache.doris.kafka.connector.converter.type;

import org.apache.doris.kafka.connector.converter.type.doris.DorisType;
import org.apache.kafka.connect.data.Schema;

public abstract class AbstractGeometryType extends AbstractType {
    @Override
    public String getTypeName(Schema schema) {
        return DorisType.STRING;
    }
}
