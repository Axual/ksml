package io.axual.ksml.schema.mapper;

import java.util.ArrayList;
import java.util.List;

import io.axual.ksml.data.type.WindowedType;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.schema.DataField;
import io.axual.ksml.schema.DataSchema;
import io.axual.ksml.schema.DataValue;
import io.axual.ksml.schema.RecordSchema;
import io.axual.ksml.schema.SchemaUtil;

public class WindowedSchemaMapper implements DataSchemaMapper<WindowedType> {
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
    private static final DataValue ZERO_LONG = new DataValue(0L);
    private static final DataValue ZERO_STRING = new DataValue("Time Zero");

    @Override
    public RecordSchema toDataSchema(WindowedType windowedType) {
        return new RecordSchema(
                WINDOW_SCHEMA_NAMESPACE,
                windowedType.schemaName(),
                WINDOW_SCHEMA_DOC_PREFIX + windowedType.keyType().schemaName(),
                generateWindowKeySchema(windowedType));
    }

    @Override
    public WindowedType fromDataSchema(DataSchema object) {
        throw new KSMLExecutionException("Can not convert a DataSchema to a Windowed dataType");
    }

    private static List<DataField> generateWindowKeySchema(WindowedType windowedType) {
        var result = new ArrayList<DataField>();
        result.add(new DataField(START_FIELD, DataSchema.create(DataSchema.Type.LONG), START_FIELD_DOC, ZERO_LONG, DataField.Order.ASCENDING));
        result.add(new DataField(END_FIELD, DataSchema.create(DataSchema.Type.LONG), END_FIELD_DOC, ZERO_LONG, DataField.Order.ASCENDING));
        result.add(new DataField(START_TIME_FIELD, DataSchema.create(DataSchema.Type.STRING), START_TIME_FIELD_DOC, ZERO_STRING, DataField.Order.ASCENDING));
        result.add(new DataField(END_TIME_FIELD, DataSchema.create(DataSchema.Type.STRING), END_TIME_FIELD_DOC, ZERO_STRING, DataField.Order.ASCENDING));
        var keySchema = SchemaUtil.dataTypeToSchema(windowedType.keyType());
        result.add(new DataField(KEY_FIELD, keySchema, KEY_FIELD_DOC, null, DataField.Order.ASCENDING));
        return result;
    }
}
