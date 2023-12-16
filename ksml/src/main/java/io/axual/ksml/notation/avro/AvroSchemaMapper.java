package io.axual.ksml.notation.avro;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2023 Axual B.V.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */

import io.axual.ksml.data.mapper.DataSchemaMapper;
import io.axual.ksml.data.schema.DataField;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.DataValue;
import io.axual.ksml.data.schema.EnumSchema;
import io.axual.ksml.data.schema.FixedSchema;
import io.axual.ksml.data.schema.ListSchema;
import io.axual.ksml.data.schema.MapSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.schema.UnionSchema;
import io.axual.ksml.data.value.Pair;
import io.axual.ksml.execution.FatalError;
import io.axual.ksml.notation.binary.NativeDataObjectMapper;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;

// First attempt at providing an internal schema class. The implementation relies heavily on Avro
// at the moment, which is fine for now, but may change in the future.
public class AvroSchemaMapper implements DataSchemaMapper<Schema> {
    private static final AvroDataMapper avroMapper = new AvroDataMapper();
    private static final NativeDataObjectMapper nativeMapper = new NativeDataObjectMapper();
    private static final Schema AVRO_NULL_TYPE = Schema.create(Schema.Type.NULL);

    @Override
    public StructSchema toDataSchema(String name, Schema schema) {
        return new StructSchema(schema.getNamespace(), schema.getName(), schema.getDoc(), convertFieldsToDataSchema(schema.getFields()));
    }

    @Override
    public Schema fromDataSchema(DataSchema schema) {
        if (schema.type() == DataSchema.Type.STRUCT) {
            var structSchema = (StructSchema) schema;
            List<Schema.Field> fields = convertFieldsToAvro(structSchema.fields());
            return Schema.createRecord(structSchema.name(), structSchema.doc(), structSchema.namespace(), false, fields);
        }
        return null;
    }

    private Pair<DataSchema, Boolean> convertToDataSchema(Schema schema) {
        return switch (schema.getType()) {
            case NULL -> Pair.of(DataSchema.create(DataSchema.Type.NULL), false);

            case BOOLEAN -> Pair.of(DataSchema.create(DataSchema.Type.BOOLEAN), true);

            case INT -> Pair.of(DataSchema.create(DataSchema.Type.INTEGER), true);
            case LONG -> Pair.of(DataSchema.create(DataSchema.Type.LONG), true);

            case BYTES -> Pair.of(DataSchema.create(DataSchema.Type.BYTES), true);
            case FIXED -> Pair.of(
                    new FixedSchema(schema.getNamespace(), schema.getName(), schema.getDoc(), schema.getFixedSize()),
                    true);

            case FLOAT -> Pair.of(DataSchema.create(DataSchema.Type.FLOAT), true);
            case DOUBLE -> Pair.of(DataSchema.create(DataSchema.Type.DOUBLE), true);

            case STRING -> Pair.of(DataSchema.create(DataSchema.Type.STRING), true);

            case ARRAY -> Pair.of(new ListSchema(convertToDataSchema(schema.getElementType()).left()), true);
            case ENUM -> Pair.of(
                    new EnumSchema(schema.getNamespace(), schema.getName(), schema.getDoc(), schema.getEnumSymbols(), schema.getEnumDefault()),
                    true);
            case MAP -> Pair.of(new MapSchema(convertToDataSchema(schema.getValueType()).left()), true);
            case RECORD -> Pair.of(toDataSchema(schema.getName(), schema), true);
            case UNION -> convertUnionToDataSchema(schema.getTypes());
        };
    }

    private Pair<DataSchema, Boolean> convertUnionToDataSchema(List<Schema> unionTypes) {
        // If a type "null" is found in AVRO schema, the respective property is considered optional, so here we detect
        // this fact and return the result Boolean as "false" indicating a required property.
        var required = true;
        if (unionTypes.size() > 1 && unionTypes.contains(AVRO_NULL_TYPE)) {
            // Create a copy of the list to prevent modifying immutable lists, then remove the NULL type
            unionTypes = new ArrayList<>(unionTypes);
            unionTypes.remove(AVRO_NULL_TYPE);
            required = false;
        }

        // If the union now contains only a single schema, then unwrap it from the union and return as simple type
        if (unionTypes.size() == 1) {
            return Pair.of(convertToDataSchema(unionTypes.get(0)).left(), required);
        }
        return Pair.of(new UnionSchema(convertToDataSchema(unionTypes).left().toArray(DataSchema[]::new)), required);
    }

    private Pair<List<DataSchema>, Boolean> convertToDataSchema(List<Schema> schemas) {
        var result = new ArrayList<DataSchema>();
        for (Schema schema : schemas) {
            result.add(convertToDataSchema(schema).left());
        }
        return Pair.of(result, true);
    }

    private List<DataField> convertFieldsToDataSchema(List<Schema.Field> fields) {
        if (fields == null) return new ArrayList<>();
        List<DataField> result = new ArrayList<>(fields.size());
        for (Schema.Field field : fields) {
            var defaultValue = convertFromAvroDefault(field);
            var property = convertToDataSchema(field.schema());
            // TODO: think about how to model fixed values in AVRO and replace the "false" with logic
            result.add(new DataField(field.name(), property.left(), field.doc(), property.right(), false, defaultValue, convertOrderFromAvro(field.order())));
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

    private Schema convertToAvro(DataSchema schema, boolean required) {
        final var result = switch (schema.type()) {
            case ANY -> FatalError.schemaError("AVRO schema do not support ANY types", Schema.class);
            case NULL -> Schema.create(Schema.Type.NULL);
            case BOOLEAN -> Schema.create(Schema.Type.BOOLEAN);
            case BYTE, SHORT, INTEGER -> Schema.create(Schema.Type.INT);
            case LONG -> Schema.create(Schema.Type.LONG);
            case DOUBLE -> Schema.create(Schema.Type.DOUBLE);
            case FLOAT -> Schema.create(Schema.Type.FLOAT);
            case BYTES -> Schema.create(Schema.Type.BYTES);
            case FIXED ->
                    Schema.createFixed(((FixedSchema) schema).name(), ((FixedSchema) schema).doc(), ((FixedSchema) schema).namespace(), ((FixedSchema) schema).size());
            case STRING -> Schema.create(Schema.Type.STRING);
            case ENUM ->
                    Schema.createEnum(((EnumSchema) schema).name(), ((EnumSchema) schema).doc(), ((EnumSchema) schema).namespace(), ((EnumSchema) schema).symbols(), ((EnumSchema) schema).defaultValue());
            case LIST -> Schema.createArray(convertToAvro(((ListSchema) schema).valueSchema(), true));
            case MAP -> Schema.createMap(convertToAvro(((MapSchema) schema).valueSchema(), true));
            case STRUCT ->
                    Schema.createRecord(((StructSchema) schema).name(), ((StructSchema) schema).doc(), ((StructSchema) schema).namespace(), false, convertFieldsToAvro(((StructSchema) schema).fields()));
            case UNION -> Schema.createUnion(convertToAvro(((UnionSchema) schema).possibleSchemas()));
        };

        // If the field is required, then return it
        if (required) return result;

        // The field is not required, so we convert the schema to a UNION, with NULL as possible element

        // If the schema is already of type UNION, then inject a NULL type at the start of array of types
        if (result.getType() == Schema.Type.UNION) {
            final var types = result.getTypes();
            // If NULL is already part of the UNION types, then return the UNION as is
            if (types.contains(AVRO_NULL_TYPE)) return result;
            // Add NULL as possible type at the start of the array
            types.add(0, AVRO_NULL_TYPE);
            return Schema.createUnion(types);
        }

        // Create a UNION with NULL as its first type
        final var schemas = new ArrayList<Schema>();
        schemas.add(AVRO_NULL_TYPE);
        schemas.add(result);
        return Schema.createUnion(schemas);
    }

    private Schema[] convertToAvro(DataSchema[] schemas) {
        var result = new Schema[schemas.length];
        for (int index = 0; index < schemas.length; index++) {
            result[index] = convertToAvro(schemas[index], true);
        }
        return result;
    }

    private List<Schema.Field> convertFieldsToAvro(List<DataField> fields) {
        if (fields == null) return new ArrayList<>();
        List<Schema.Field> result = new ArrayList<>(fields.size());
        for (DataField field : fields) {
            result.add(convertToAvro(field));
        }
        return result;
    }

    private Schema.Field convertToAvro(DataField field) {
        var defaultValue = convertDefaultValue(field.defaultValue());
        return new Schema.Field(field.name(), convertToAvro(field.schema(), field.required()), field.doc(), defaultValue, convertOrderToAvro(field.order()));
    }

    private DataValue convertFromAvroDefault(Schema.Field field) {
        if (!field.hasDefaultValue()) return null;
        var value = nativeMapper.fromDataObject(avroMapper.toDataObject(field.defaultVal()));
        return new DataValue(value);
    }

    private Object convertDefaultValue(DataValue value) {
        if (value == null) return null;
        if (value.value() == null) return Schema.Field.NULL_DEFAULT_VALUE;
        return value.value();
    }

    private Schema.Field.Order convertOrderToAvro(DataField.Order order) {
        return switch (order) {
            case ASCENDING -> Schema.Field.Order.ASCENDING;
            case DESCENDING -> Schema.Field.Order.DESCENDING;
            default -> Schema.Field.Order.IGNORE;
        };
    }
}
