package org.apache.doris.kafka.connector.converter.type.util;

import com.esri.core.geometry.ogc.OGCGeometry;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeoUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(GeoUtils.class);

    private GeoUtils() {}

    public static Object handleGeoStructData(Object sourceValue) {
        // the Geometry datatype in MySQL will be converted to
        // a String with Json format
        final ObjectMapper objectMapper = new ObjectMapper();
        final ObjectWriter objectWriter = objectMapper.writer();
        Struct geometryStruct = (Struct) sourceValue;

        try {
            byte[] wkb = geometryStruct.getBytes("wkb");
            String geoJson = OGCGeometry.fromBinary(ByteBuffer.wrap(wkb)).asGeoJson();
            JsonNode originGeoNode = objectMapper.readTree(geoJson);

            Optional<Integer> srid = Optional.ofNullable(geometryStruct.getInt32("srid"));
            Map<String, Object> geometryInfo = new HashMap<>();
            String geometryType = originGeoNode.get("type").asText();

            geometryInfo.put("type", geometryType);
            if ("GeometryCollection".equals(geometryType)) {
                geometryInfo.put("geometries", originGeoNode.get("geometries"));
            } else {
                geometryInfo.put("coordinates", originGeoNode.get("coordinates"));
            }

            geometryInfo.put("srid", srid.orElse(0));
            return objectWriter.writeValueAsString(geometryInfo);
        } catch (Exception e) {
            LOGGER.warn("Failed to parse Geometry datatype, converting the value to null", e);
            return null;
        }
    }
}
