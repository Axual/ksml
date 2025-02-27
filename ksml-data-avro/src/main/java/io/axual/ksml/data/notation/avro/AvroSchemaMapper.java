package io.axual.ksml.data.notation.avro;

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

import io.axual.ksml.data.exception.SchemaException;
import io.axual.ksml.data.mapper.DataSchemaMapper;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.schema.*;
import io.axual.ksml.data.type.Symbol;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.axual.ksml.data.schema.DataField.NO_TAG;

public class AvroSchemaMapper implements DataSchemaMapper<Schema> {
    private static final AvroDataObjectMapper avroMapper = new AvroDataObjectMapper();
    private static final Schema AVRO_NULL_TYPE = Schema.create(Schema.Type.NULL);
    private static final NativeDataObjectMapper NATIVE_MAPPER = new NativeDataObjectMapper();

    @Override
    public StructSchema toDataSchema(String namespace, String name, Schema schema) {
        // The namespace and name fields are ignored, since they are already contained in the schema and
        // take precedence over the parameters to this method.
        return new StructSchema(schema.getNamespace(), schema.getName(), schema.getDoc(), convertAvroFieldsToDataFields(schema.getFields()));
    }

    @Override
    public Schema fromDataSchema(DataSchema schema) {
        if (schema instanceof StructSchema structSchema) {
            final var fields = convertFieldsToAvroFields(structSchema.fields());
            return Schema.createRecord(structSchema.name(), structSchema.doc(), structSchema.namespace(), false, fields);
        }
        return null;
    }

    private record SchemaAndRequired(DataSchema schema, boolean required) {
    }

    private record AvroSchemaAndDefaultValue(Schema schema, DataValue defaultValue) {
    }

    private SchemaAndRequired convertAvroSchemaToDataSchemaAndRequired(Schema schema) {
        // Returns a record with
        //   1. the DataSchema representation of the schema parameter
        //   2. a boolean indicating whether the field is required
        return switch (schema.getType()) {
            case NULL -> new SchemaAndRequired(DataSchema.NULL_SCHEMA, false);

            case BOOLEAN -> new SchemaAndRequired(DataSchema.BOOLEAN_SCHEMA, true);

            case INT -> new SchemaAndRequired(DataSchema.INTEGER_SCHEMA, true);
            case LONG -> new SchemaAndRequired(DataSchema.LONG_SCHEMA, true);

            case FLOAT -> new SchemaAndRequired(DataSchema.FLOAT_SCHEMA, true);
            case DOUBLE -> new SchemaAndRequired(DataSchema.DOUBLE_SCHEMA, true);

            case BYTES -> new SchemaAndRequired(DataSchema.BYTES_SCHEMA, true);
            case FIXED -> new SchemaAndRequired(
                    new FixedSchema(schema.getNamespace(), schema.getName(), schema.getDoc(), schema.getFixedSize()),
                    true);

            case STRING -> new SchemaAndRequired(DataSchema.STRING_SCHEMA, true);

            case ARRAY ->
                    new SchemaAndRequired(new ListSchema(convertAvroSchemaToDataSchemaAndRequired(schema.getElementType()).schema()), true);
            case ENUM -> new SchemaAndRequired(
                    new EnumSchema(schema.getNamespace(), schema.getName(), schema.getDoc(), schema.getEnumSymbols().stream().map(Symbol::new).toList(), schema.getEnumDefault()),
                    true);
            case MAP ->
                    new SchemaAndRequired(new MapSchema(convertAvroSchemaToDataSchemaAndRequired(schema.getValueType()).schema()), true);
            case RECORD -> new SchemaAndRequired(toDataSchema(schema.getName(), schema), true);
            case UNION -> convertMemberSchemasToToDataUnionAndRequired(schema.getTypes());
        };
    }

    private SchemaAndRequired convertMemberSchemasToToDataUnionAndRequired(List<Schema> unionTypes) {
        // If a type "null" is found in AVRO schema, the respective property is considered optional, so here we detect
        // this fact and return the result Boolean as "false" indicating a required property.
        if (unionTypes.size() > 1 && unionTypes.contains(AVRO_NULL_TYPE)) {
            // Create a copy of the list to prevent modifying immutable lists, then remove the NULL type
            unionTypes = new ArrayList<>(unionTypes);
            unionTypes.remove(AVRO_NULL_TYPE);

            // If the union now contains only a single schema, then unwrap it from the union and return as simple type
            if (unionTypes.size() == 1) {
                return new SchemaAndRequired(convertAvroSchemaToDataSchemaAndRequired(unionTypes.getFirst()).schema(), false);
            }
            return new SchemaAndRequired(new UnionSchema(convertAvroSchemaToDataFields(unionTypes).toArray(DataField[]::new)), false);
        }

        return new SchemaAndRequired(new UnionSchema(convertAvroSchemaToDataFields(unionTypes).toArray(DataField[]::new)), true);
    }

    private List<DataField> convertAvroSchemaToDataFields(List<Schema> schemas) {
        final var result = new ArrayList<DataField>();
        for (Schema schema : schemas) {
            result.add(new DataField(convertAvroSchemaToDataSchemaAndRequired(schema).schema()));
        }
        return result;
    }

    private List<DataField> convertAvroFieldsToDataFields(List<Schema.Field> fields) {
        if (fields == null) return new ArrayList<>();
        final var result = new ArrayList<DataField>(fields.size());
        for (Schema.Field field : fields) {
            final var schemaAndRequired = convertAvroSchemaToDataSchemaAndRequired(field.schema());
            final var convertedDefault = convertAvroDefaultValueToDataValue(field);
            final var defaultValue = schemaAndRequired.required() || (convertedDefault != null && convertedDefault.value() != null) ? convertedDefault : null;
            // TODO: think about how to model fixed values in AVRO and replace the "false" with logic
            result.add(new DataField(field.name(), schemaAndRequired.schema(), field.doc(), NO_TAG, schemaAndRequired.required(), false, defaultValue, convertAvroOrderToDataFieldOrder(field.order())));
        }
        return result;
    }

    private static DataField.Order convertAvroOrderToDataFieldOrder(Schema.Field.Order order) {
        return switch (order) {
            case ASCENDING -> DataField.Order.ASCENDING;
            case DESCENDING -> DataField.Order.DESCENDING;
            default -> DataField.Order.IGNORE;
        };
    }

    private Schema convertDataSchemaToAvroSchema(DataSchema schema) {
        if (schema == DataSchema.ANY_SCHEMA) throw new SchemaException("AVRO schema do not support ANY types");
        if (schema == DataSchema.NULL_SCHEMA) return Schema.create(Schema.Type.NULL);
        if (schema == DataSchema.BOOLEAN_SCHEMA) return Schema.create(Schema.Type.BOOLEAN);
        if (schema == DataSchema.BYTE_SCHEMA || schema == DataSchema.SHORT_SCHEMA || schema == DataSchema.INTEGER_SCHEMA)
            return Schema.create(Schema.Type.INT);
        if (schema == DataSchema.LONG_SCHEMA) return Schema.create(Schema.Type.LONG);
        if (schema == DataSchema.FLOAT_SCHEMA) return Schema.create(Schema.Type.FLOAT);
        if (schema == DataSchema.DOUBLE_SCHEMA) return Schema.create(Schema.Type.DOUBLE);
        if (schema == DataSchema.BYTES_SCHEMA) return Schema.create(Schema.Type.BYTES);
        if (schema instanceof FixedSchema fixedSchema)
            return Schema.createFixed(fixedSchema.name(), fixedSchema.doc(), fixedSchema.namespace(), fixedSchema.size());
        if (schema == DataSchema.STRING_SCHEMA) return Schema.create(Schema.Type.STRING);
        if (schema instanceof EnumSchema enumSchema)
            return Schema.createEnum(enumSchema.name(), enumSchema.doc(), enumSchema.namespace(), enumSchema.symbols().stream().map(Symbol::name).toList(), enumSchema.defaultValue());
        if (schema instanceof ListSchema listSchema)
            return Schema.createArray(convertDataSchemaToAvroSchema(listSchema.valueSchema(), true).schema());
        if (schema instanceof MapSchema mapSchema)
            return Schema.createMap(convertDataSchemaToAvroSchema(mapSchema.valueSchema(), true).schema());
        if (schema instanceof StructSchema structSchema)
            return Schema.createRecord(structSchema.name(), structSchema.doc(), structSchema.namespace(), false, convertFieldsToAvroFields(structSchema.fields()));
        if (schema instanceof UnionSchema unionSchema)
            return Schema.createUnion(convertUnionMemberSchemasToAvro(Arrays.stream(unionSchema.memberSchemas()).map(DataField::schema).toArray(DataSchema[]::new)));
        throw new SchemaException("Can not convert schema to AVRO: " + schema);
    }

    private AvroSchemaAndDefaultValue convertDataSchemaToAvroSchema(DataSchema schema, boolean required) {
        final var result = convertDataSchemaToAvroSchema(schema);

        // If the field is required, then return it
        if (required) return new AvroSchemaAndDefaultValue(result, null);

        // The field is not required, so we convert the schema to a UNION, with NULL as first possible type
        final var defaultValue = new DataValue(null);

        // If the schema is already of type UNION, then inject a NULL type at the start of array of types
        if (result.getType() == Schema.Type.UNION) {
            final var types = result.getTypes();
            // If NULL is already part of the UNION types, then return the UNION as is
            if (types.contains(AVRO_NULL_TYPE)) return new AvroSchemaAndDefaultValue(result, defaultValue);
            // Add NULL as possible value type at the start of the array
            types.addFirst(AVRO_NULL_TYPE);
            return new AvroSchemaAndDefaultValue(Schema.createUnion(types), defaultValue);
        }

        // Create a UNION with NULL as its first type
        final var schemas = new ArrayList<Schema>();
        schemas.add(AVRO_NULL_TYPE);
        schemas.add(result);
        return new AvroSchemaAndDefaultValue(Schema.createUnion(schemas), defaultValue);
    }

    private Schema[] convertUnionMemberSchemasToAvro(DataSchema[] schemas) {
        final var result = new Schema[schemas.length];
        for (int index = 0; index < schemas.length; index++) {
            result[index] = convertDataSchemaToAvroSchema(schemas[index], true).schema();
        }
        return result;
    }

    private List<Schema.Field> convertFieldsToAvroFields(List<DataField> fields) {
        if (fields == null) return new ArrayList<>();
        final var result = new ArrayList<Schema.Field>(fields.size());
        for (DataField field : fields) {
            result.add(convertDataFieldToAvroField(field));
        }
        return result;
    }

    private Schema.Field convertDataFieldToAvroField(DataField field) {
        final var schemaAndDefault = convertDataSchemaToAvroSchema(field.schema(), field.required());
        final var defaultValue = field.defaultValue() != null ? field.defaultValue() : schemaAndDefault.defaultValue();
        final var defaultAvroValue = convertDataValueToAvroDefaultValue(defaultValue);
        return new Schema.Field(field.name(), schemaAndDefault.schema(), field.doc(), defaultAvroValue, convertDataFieldOrderToAvroFieldOrder(field.order()));
    }

    private DataValue convertAvroDefaultValueToDataValue(Schema.Field field) {
        if (!field.hasDefaultValue()) return null;
        final var value = NATIVE_MAPPER.fromDataObject(avroMapper.toDataObject(field.defaultVal()));
        return new DataValue(value);
    }

    private Object convertDataValueToAvroDefaultValue(DataValue value) {
        if (value == null) return null;
        if (value.value() == null) return Schema.Field.NULL_DEFAULT_VALUE;
        return value.value();
    }

    private Schema.Field.Order convertDataFieldOrderToAvroFieldOrder(DataField.Order order) {
        return switch (order) {
            case ASCENDING -> Schema.Field.Order.ASCENDING;
            case DESCENDING -> Schema.Field.Order.DESCENDING;
            default -> Schema.Field.Order.IGNORE;
        };
    }
}
