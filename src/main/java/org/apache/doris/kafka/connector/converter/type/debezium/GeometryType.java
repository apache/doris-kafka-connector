package org.apache.doris.kafka.connector.converter.type.debezium;

import com.esri.core.geometry.GeometryException;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.debezium.data.geometry.Geometry;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.doris.kafka.connector.converter.type.AbstractGeometryType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeometryType extends AbstractGeometryType {
    private static final Logger LOGGER = LoggerFactory.getLogger(GeometryType.class);
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
            // the Geometry datatype in MySQL will be converted to
            // a String with Json format
            final ObjectMapper objectMapper = new ObjectMapper();
            final ObjectWriter objectWriter = objectMapper.writer();
            Struct geometryStruct = (Struct) sourceValue;
            byte[] wkb = geometryStruct.getBytes("wkb");
            JsonNode originGeoNode = null;
            try {
                String geoJson = OGCGeometry.fromBinary(ByteBuffer.wrap(wkb)).asGeoJson();
                originGeoNode = objectMapper.readTree(geoJson);
            } catch (JsonProcessingException | GeometryException e) {
                LOGGER.warn("parse Geometry type failed ,convert the value to null ");
                return null;
            }
            Optional<Integer> srid = Optional.ofNullable(geometryStruct.getInt32("srid"));
            Map<String, Object> geometryInfo = new HashMap<>();
            String geometryType = originGeoNode.get("type").asText();
            geometryInfo.put("type", geometryType);
            if (geometryType.equals("GeometryCollection")) {
                geometryInfo.put("geometries", originGeoNode.get("geometries"));
            } else {
                geometryInfo.put("coordinates", originGeoNode.get("coordinates"));
            }
            geometryInfo.put("srid", srid.orElse(0));
            try {
                return objectWriter.writeValueAsString(geometryInfo);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        return sourceValue.toString();
    }

    @Override
    protected Optional<String> getSchemaParameter(Schema schema, String parameterName) {
        if (!Objects.isNull(schema.parameters())) {
            return Optional.ofNullable(schema.parameters().get(parameterName));
        }
        return Optional.empty();
    }
}
