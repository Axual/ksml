package io.axual.ksml.dsl;

import io.axual.ksml.data.schema.*;

import java.util.ArrayList;

public class HeaderSchema {
    private HeaderSchema() {
    }

    // Public constants are the fixed schemas and the field names
    public static final StructSchema HEADER_SCHEMA = generateHeaderSchema();
    public static final String HEADER_SCHEMA_NAME = "Header";
    public static final String HEADER_SCHEMA_KEY_FIELD = "key";
    public static final String HEADER_SCHEMA_VALUE_FIELD = "value";
    private static final String HEADER_SCHEMA_KEY_DOC = "Header key";
    private static final String HEADER_SCHEMA_VALUE_DOC = "Header value";
    private static final String KAFKA_PREFIX = "Kafka ";

    private static StructSchema generateHeaderSchema() {
        final var headerFields = new ArrayList<DataField>();
        headerFields.add(new DataField(HEADER_SCHEMA_KEY_FIELD, DataSchema.stringSchema(), HEADER_SCHEMA_KEY_DOC, 1));
        headerFields.add(new DataField(HEADER_SCHEMA_VALUE_FIELD, AnySchema.INSTANCE, HEADER_SCHEMA_VALUE_DOC, 2));
        return new StructSchema(DataSchemaConstants.DATA_SCHEMA_KSML_NAMESPACE, HEADER_SCHEMA_NAME, KAFKA_PREFIX + HEADER_SCHEMA_NAME, headerFields);
    }
}
