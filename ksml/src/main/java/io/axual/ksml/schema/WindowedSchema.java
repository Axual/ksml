package io.axual.ksml.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.type.WindowedType;

public class WindowedSchema extends RecordSchema {
    private static final String WINDOW_SCHEMA_DOC_PREFIX = "Windowed ";
    private static final String WINDOW_SCHEMA_NAMESPACE = "io.axual.ksml.data";
    public static final String START_FIELD = "start";
    private static final String START_FIELD_DOC = "Start timestamp";
    public static final String END_FIELD = "end";
    private static final String END_FIELD_DOC = "End timestamp";
    public static final String START_TIME_FIELD = "startTime";
    private static final String START_TIME_FIELD_DOC = "Start time";
    public static final String END_TIME_FIELD = "endTime";
    private static final String END_TIME_FIELD_DOC = "End time";
    public static final String KEY_FIELD = "key";
    private static final String KEY_FIELD_DOC = "Window key";
    private static final DataLong ZERO_LONG = new DataLong(0L);
    private static final DataString ZERO_STRING = new DataString("Time Zero");
    private final WindowedType windowedType;

    public WindowedSchema(WindowedType windowedType) {
        super(WINDOW_SCHEMA_NAMESPACE,
                windowedType.schemaName(),
                WINDOW_SCHEMA_DOC_PREFIX + windowedType.keyType().schemaName(),
                generateWindowKeySchema(windowedType));
        this.windowedType = windowedType;
    }

    private static List<DataField> generateWindowKeySchema(WindowedType windowedType) {
        var result = new ArrayList<DataField>();
        result.add(new DataField(START_FIELD, DataSchema.create(Type.LONG), START_FIELD_DOC, ZERO_LONG, DataField.Order.ASCENDING));
        result.add(new DataField(END_FIELD, DataSchema.create(Type.LONG), END_FIELD_DOC, ZERO_LONG, DataField.Order.ASCENDING));
        result.add(new DataField(START_TIME_FIELD, DataSchema.create(Type.STRING), START_TIME_FIELD_DOC, ZERO_STRING, DataField.Order.ASCENDING));
        result.add(new DataField(END_TIME_FIELD, DataSchema.create(Type.STRING), END_TIME_FIELD_DOC, ZERO_STRING, DataField.Order.ASCENDING));
        var keySchema = SchemaUtil.dataTypeToSchema(windowedType.keyType());
        result.add(new DataField(KEY_FIELD, keySchema, KEY_FIELD_DOC, null, DataField.Order.ASCENDING));
        return result;
    }

    public WindowedType windowedType() {
        return windowedType;
    }

    @Override
    public boolean equals(Object other) {
        if (!super.equals(other)) return false;
        if (this == other) return true;
        if (other.getClass() != getClass()) return false;

        // Compare all schema relevant fields, note: explicitly do not compare the doc field
        return Objects.equals(windowedType, ((WindowedSchema) other).windowedType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), windowedType);
    }
}
