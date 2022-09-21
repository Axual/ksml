package io.axual.ksml.avro;

import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;

import io.axual.ksml.schema.ArraySchema;
import io.axual.ksml.schema.DataField;
import io.axual.ksml.schema.DataSchema;
import io.axual.ksml.schema.EnumSchema;
import io.axual.ksml.schema.MapSchema;
import io.axual.ksml.schema.NamedSchema;
import io.axual.ksml.schema.NullableSchema;
import io.axual.ksml.schema.RecordSchema;

public class AvroUtil {
    private static final AvroDataMapper mapper = new AvroDataMapper();

    public static DataSchema convertSchemaFromAvro(Schema schema) {
        return switch (schema.getType()) {
            case BOOLEAN -> DataSchema.create(DataSchema.Type.BOOLEAN);
            case RECORD -> new AvroSchema(schema);
            case ENUM -> new EnumSchema(schema.getNamespace(), schema.getName(), schema.getDoc(), schema.getEnumSymbols(), schema.getEnumDefault());
            case ARRAY -> new ArraySchema(convertSchemaFromAvro(schema.getValueType()));
            case MAP -> new MapSchema(convertSchemaFromAvro(schema.getValueType()));
            case STRING -> DataSchema.create(DataSchema.Type.STRING);
            case BYTES -> DataSchema.create(DataSchema.Type.BYTES);
            case INT -> DataSchema.create(DataSchema.Type.INTEGER);
            case LONG -> DataSchema.create(DataSchema.Type.LONG);
            case FLOAT -> DataSchema.create(DataSchema.Type.FLOAT);
            case DOUBLE -> DataSchema.create(DataSchema.Type.DOUBLE);
            default -> throw new IllegalStateException("Unexpected value: " + schema.getType());
        };
    }

    public static List<DataField> convertFieldsFromAvro(List<Schema.Field> fields) {
        if (fields == null) return new ArrayList<>();
        List<DataField> result = new ArrayList<>(fields.size());
        for (Schema.Field field : fields) {
            result.add(new DataField(field.name(), convertSchemaFromAvro(field.schema()), field.doc(), mapper.toDataObject(field.defaultVal()), convertOrderFromAvro(field.order())));
        }
        return result;
    }

    private static DataField.Order convertOrderFromAvro(Schema.Field.Order order) {
        return switch (order) {
            case ASCENDING -> DataField.Order.ASCENDING;
            case DESCENDING -> DataField.Order.DESCENDING;
            default -> DataField.Order.IGNORE;
        };
    }

    public static Schema convertSchemaToAvro(DataSchema schema) {
        return switch (schema.type()) {
            case BOOLEAN -> Schema.create(Schema.Type.BOOLEAN);
            case BYTE, SHORT, INTEGER -> Schema.create(Schema.Type.INT);
            case BYTES -> Schema.create(Schema.Type.BYTES);
            case DOUBLE -> Schema.create(Schema.Type.DOUBLE);
            case FLOAT -> Schema.create(Schema.Type.FLOAT);
            case LONG -> Schema.create(Schema.Type.LONG);
            case STRING -> Schema.create(Schema.Type.STRING);
            case ARRAY -> Schema.createArray(convertSchemaToAvro(((ArraySchema) schema).valueType()));
            case ENUM -> Schema.createEnum(((EnumSchema) schema).name(), ((EnumSchema) schema).doc(), ((EnumSchema) schema).namespace(), ((EnumSchema) schema).values(), ((EnumSchema)schema).defaultValue());
            case MAP -> Schema.createMap(convertSchemaToAvro(((MapSchema) schema).valueType()));
            case RECORD -> Schema.createRecord((schema instanceof NamedSchema ns ? ns.name() : ""), ((RecordSchema) schema).doc(), ((RecordSchema) schema).namespace(), false, convertFieldsToAvro(((RecordSchema) schema).fields()));
            case NULLABLE -> Schema.createUnion(Schema.create(Schema.Type.NULL), convertSchemaToAvro(((NullableSchema) schema).valueType()));
        };
    }

    public static List<Schema.Field> convertFieldsToAvro(List<DataField> fields) {
        if (fields == null) return new ArrayList<>();
        List<Schema.Field> result = new ArrayList<>(fields.size());
        for (DataField field : fields) {
            result.add(convertFieldToAvro(field));
        }
        return result;
    }

    private static Schema.Field convertFieldToAvro(DataField field) {
        return new Schema.Field(field.name(), convertSchemaToAvro(field.schema()), field.doc(), field.defaultValue(), convertOrder(field.order()));
    }

    private static Schema.Field.Order convertOrder(DataField.Order order) {
        return switch (order) {
            case ASCENDING -> Schema.Field.Order.ASCENDING;
            case DESCENDING -> Schema.Field.Order.DESCENDING;
            default -> Schema.Field.Order.IGNORE;
        };
    }
}
