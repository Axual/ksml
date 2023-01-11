package io.axual.ksml.dsl;

import java.util.ArrayList;

import io.axual.ksml.schema.DataField;
import io.axual.ksml.schema.DataSchema;
import io.axual.ksml.schema.ListSchema;
import io.axual.ksml.schema.StructSchema;

public class StandardSchemas {
    public static final StructSchema RECORD_CONTEXT_HEADER_SCHEMA = generateRecordContextHeaderSchema();
    public static final StructSchema RECORD_CONTEXT_SCHEMA = generateRecordContextSchema();
    public static final String RECORD_CONTEXT_HEADERS = "headers";
    public static final String RECORD_CONTEXT_OFFSET = "offset";
    public static final String RECORD_CONTEXT_PARTITION = "partition";
    public static final String RECORD_CONTEXT_TIMESTAMP = "timestamp";
    public static final String RECORD_CONTEXT_TOPIC = "topic";
    public static final String RECORD_CONTEXT_KEY = "key";
    public static final String RECORD_CONTEXT_VALUE = "value";


    private static StructSchema generateRecordContextSchema() {
        var fields = new ArrayList<DataField>();
        fields.add(new DataField(RECORD_CONTEXT_OFFSET, DataSchema.create(DataSchema.Type.LONG), "Message offset", null, DataField.Order.ASCENDING));
        fields.add(new DataField(RECORD_CONTEXT_TIMESTAMP, DataSchema.create(DataSchema.Type.LONG), "Message timestamp", null, DataField.Order.ASCENDING));
        fields.add(new DataField(RECORD_CONTEXT_TOPIC, DataSchema.create(DataSchema.Type.STRING), "Message topic", null, DataField.Order.ASCENDING));
        fields.add(new DataField(RECORD_CONTEXT_PARTITION, DataSchema.create(DataSchema.Type.INTEGER), "Message partition", null, DataField.Order.ASCENDING));
        fields.add(new DataField(RECORD_CONTEXT_HEADERS, new ListSchema(RECORD_CONTEXT_HEADER_SCHEMA), "Message headers", null, DataField.Order.ASCENDING));
        return new StructSchema("io.axual.ksml.data", "RecordContext", "RecordContext struct", fields);
    }

    private static StructSchema generateRecordContextHeaderSchema() {
        var headerFields = new ArrayList<DataField>();
        headerFields.add(new DataField(RECORD_CONTEXT_KEY, DataSchema.create(DataSchema.Type.STRING), "Header key", null, DataField.Order.ASCENDING));
        headerFields.add(new DataField(RECORD_CONTEXT_VALUE, DataSchema.create(DataSchema.Type.BYTES), "Header value", null, DataField.Order.ASCENDING));
        return new StructSchema("io.axual.ksml.data", "Header", "Header struct", headerFields);
    }
}
