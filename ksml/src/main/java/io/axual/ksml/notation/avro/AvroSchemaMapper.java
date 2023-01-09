package io.axual.ksml.notation.avro;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 Axual B.V.
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

import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;

import io.axual.ksml.data.mapper.DataSchemaMapper;
import io.axual.ksml.notation.binary.NativeDataObjectMapper;
import io.axual.ksml.schema.DataField;
import io.axual.ksml.schema.DataSchema;
import io.axual.ksml.schema.DataValue;
import io.axual.ksml.schema.EnumSchema;
import io.axual.ksml.schema.FixedSchema;
import io.axual.ksml.schema.ListSchema;
import io.axual.ksml.schema.MapSchema;
import io.axual.ksml.schema.StructSchema;
import io.axual.ksml.schema.UnionSchema;

// First attempt at providing an internal schema class. The implementation relies heavily on Avro
// at the moment, which is fine for now, but may change in the future.
public class AvroSchemaMapper implements DataSchemaMapper<Schema> {
    private static final AvroDataMapper avroMapper = new AvroDataMapper();
    private static final NativeDataObjectMapper nativeMapper = new NativeDataObjectMapper();

    @Override
    public StructSchema toDataSchema(Schema schema) {
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

    private DataSchema convertToDataSchema(Schema schema) {
        return switch (schema.getType()) {
            case NULL -> DataSchema.create(DataSchema.Type.NULL);

            case BOOLEAN -> DataSchema.create(DataSchema.Type.BOOLEAN);

            case INT -> DataSchema.create(DataSchema.Type.INTEGER);
            case LONG -> DataSchema.create(DataSchema.Type.LONG);

            case BYTES -> DataSchema.create(DataSchema.Type.BYTES);
            case FIXED -> new FixedSchema(schema.getNamespace(), schema.getName(), schema.getDoc(), schema.getFixedSize());

            case FLOAT -> DataSchema.create(DataSchema.Type.FLOAT);
            case DOUBLE -> DataSchema.create(DataSchema.Type.DOUBLE);

            case STRING -> DataSchema.create(DataSchema.Type.STRING);

            case ARRAY -> new ListSchema(convertToDataSchema(schema.getElementType()));
            case ENUM -> new EnumSchema(schema.getNamespace(), schema.getName(), schema.getDoc(), schema.getEnumSymbols(), schema.getEnumDefault());
            case MAP -> new MapSchema(convertToDataSchema(schema.getValueType()));
            case RECORD -> toDataSchema(schema);
            case UNION -> new UnionSchema(convertToDataSchema(schema.getTypes()).toArray(new DataSchema[0]));
        };
    }

    private List<DataSchema> convertToDataSchema(List<Schema> schemas) {
        var result = new ArrayList<DataSchema>();
        for (Schema schema : schemas) {
            result.add(convertToDataSchema(schema));
        }
        return result;
    }

    private List<DataField> convertFieldsToDataSchema(List<Schema.Field> fields) {
        if (fields == null) return new ArrayList<>();
        List<DataField> result = new ArrayList<>(fields.size());
        for (Schema.Field field : fields) {
            var defaultValue = convertFromAvroDefault(field);
            result.add(new DataField(field.name(), convertToDataSchema(field.schema()), field.doc(), defaultValue, convertOrderFromAvro(field.order())));
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

    private Schema convertToAvro(DataSchema schema) {
        return switch (schema.type()) {
            case NULL -> Schema.create(Schema.Type.NULL);
            case BOOLEAN -> Schema.create(Schema.Type.BOOLEAN);
            case BYTE, SHORT, INTEGER -> Schema.create(Schema.Type.INT);
            case LONG -> Schema.create(Schema.Type.LONG);
            case DOUBLE -> Schema.create(Schema.Type.DOUBLE);
            case FLOAT -> Schema.create(Schema.Type.FLOAT);
            case BYTES -> Schema.create(Schema.Type.BYTES);
            case FIXED -> Schema.createFixed(((FixedSchema) schema).name(), ((FixedSchema) schema).doc(), ((FixedSchema) schema).namespace(), ((FixedSchema) schema).size());
            case STRING -> Schema.create(Schema.Type.STRING);
            case ENUM -> Schema.createEnum(((EnumSchema) schema).name(), ((EnumSchema) schema).doc(), ((EnumSchema) schema).namespace(), ((EnumSchema) schema).symbols(), ((EnumSchema) schema).defaultValue());
            case LIST -> Schema.createArray(convertToAvro(((ListSchema) schema).valueType()));
            case MAP -> Schema.createMap(convertToAvro(((MapSchema) schema).valueSchema()));
            case STRUCT -> Schema.createRecord(((StructSchema) schema).name(), ((StructSchema) schema).doc(), ((StructSchema) schema).namespace(), false, convertFieldsToAvro(((StructSchema) schema).fields()));
            case UNION -> Schema.createUnion(convertToAvro(((UnionSchema) schema).possibleSchema()));
        };
    }

    private Schema[] convertToAvro(DataSchema[] schemas) {
        var result = new Schema[schemas.length];
        for (int index = 0; index < schemas.length; index++) {
            result[index] = convertToAvro(schemas[index]);
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
        return new Schema.Field(field.name(), convertToAvro(field.schema()), field.doc(), defaultValue, convertOrderToAvro(field.order()));
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
