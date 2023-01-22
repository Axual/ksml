package io.axual.ksml.notation.binary;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2022 Axual B.V.
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import io.axual.ksml.data.mapper.DataSchemaMapper;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.parser.YamlNode;
import io.axual.ksml.data.schema.DataField;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.DataValue;
import io.axual.ksml.data.schema.EnumSchema;
import io.axual.ksml.data.schema.FixedSchema;
import io.axual.ksml.data.schema.ListSchema;
import io.axual.ksml.data.schema.MapSchema;
import io.axual.ksml.data.schema.NamedSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.schema.UnionSchema;
import io.axual.ksml.parser.schema.DataSchemaParser;

import static io.axual.ksml.dsl.DataSchemaDSL.ANY_TYPE;
import static io.axual.ksml.dsl.DataSchemaDSL.BOOLEAN_TYPE;
import static io.axual.ksml.dsl.DataSchemaDSL.BYTES_TYPE;
import static io.axual.ksml.dsl.DataSchemaDSL.BYTE_TYPE;
import static io.axual.ksml.dsl.DataSchemaDSL.DATA_FIELD_DEFAULT_VALUE_FIELD;
import static io.axual.ksml.dsl.DataSchemaDSL.DATA_FIELD_DOC_FIELD;
import static io.axual.ksml.dsl.DataSchemaDSL.DATA_FIELD_NAME_FIELD;
import static io.axual.ksml.dsl.DataSchemaDSL.DATA_FIELD_ORDER_FIELD;
import static io.axual.ksml.dsl.DataSchemaDSL.DATA_FIELD_SCHEMA_FIELD;
import static io.axual.ksml.dsl.DataSchemaDSL.DATA_SCHEMA_TYPE_FIELD;
import static io.axual.ksml.dsl.DataSchemaDSL.DOUBLE_TYPE;
import static io.axual.ksml.dsl.DataSchemaDSL.ENUM_SCHEMA_DEFAULTVALUE_FIELD;
import static io.axual.ksml.dsl.DataSchemaDSL.ENUM_SCHEMA_POSSIBLEVALUES_FIELD;
import static io.axual.ksml.dsl.DataSchemaDSL.FIXED_SCHEMA_SIZE_FIELD;
import static io.axual.ksml.dsl.DataSchemaDSL.FIXED_TYPE;
import static io.axual.ksml.dsl.DataSchemaDSL.FLOAT_TYPE;
import static io.axual.ksml.dsl.DataSchemaDSL.INTEGER_TYPE;
import static io.axual.ksml.dsl.DataSchemaDSL.LIST_SCHEMA_VALUES_FIELD;
import static io.axual.ksml.dsl.DataSchemaDSL.LONG_TYPE;
import static io.axual.ksml.dsl.DataSchemaDSL.MAP_SCHEMA_VALUES_FIELD;
import static io.axual.ksml.dsl.DataSchemaDSL.NAMED_SCHEMA_DOC_FIELD;
import static io.axual.ksml.dsl.DataSchemaDSL.NAMED_SCHEMA_NAMESPACE_FIELD;
import static io.axual.ksml.dsl.DataSchemaDSL.NAMED_SCHEMA_NAME_FIELD;
import static io.axual.ksml.dsl.DataSchemaDSL.NULL_TYPE;
import static io.axual.ksml.dsl.DataSchemaDSL.SHORT_TYPE;
import static io.axual.ksml.dsl.DataSchemaDSL.STRING_TYPE;
import static io.axual.ksml.dsl.DataSchemaDSL.STRUCT_SCHEMA_FIELDS_FIELD;

public class NativeDataSchemaMapper implements DataSchemaMapper<Object> {
    private static final DataSchemaParser PARSER = new DataSchemaParser();
    private static final NativeJsonNodeMapper JSON_NODE_MAPPER = new NativeJsonNodeMapper();

    @Override
    public DataSchema toDataSchema(Object value) {
        var json = JSON_NODE_MAPPER.toJsonNode(value);
        var root = YamlNode.fromRoot(json, "Schema");
        return PARSER.parse(root);
    }

    @Override
    public Object fromDataSchema(DataSchema schema) {
        if (schema instanceof UnionSchema unionSchema) {
            var result = new ArrayList<>();
            for (DataSchema possibleSchema : unionSchema.possibleSchemas())
                result.add(convertSchema(possibleSchema));
            return result;
        }

        var result = new HashMap<String, Object>();
        result.put(DATA_SCHEMA_TYPE_FIELD, schema.type().toString().toLowerCase());
        if (schema instanceof NamedSchema namedSchema)
            writeNamedSchemaToMap(result, namedSchema);
        if (schema instanceof ListSchema listSchema)
            result.put(LIST_SCHEMA_VALUES_FIELD, convertSchema(listSchema.valueSchema()));
        if (schema instanceof MapSchema mapSchema)
            result.put(MAP_SCHEMA_VALUES_FIELD, convertSchema(mapSchema.valueSchema()));
        return result;
    }

    private void writeNamedSchemaToMap(Map<String, Object> result, NamedSchema namedSchema) {
        if (namedSchema.namespace() != null)
            result.put(NAMED_SCHEMA_NAMESPACE_FIELD, namedSchema.namespace());
        result.put(NAMED_SCHEMA_NAME_FIELD, namedSchema.name());
        if (namedSchema.doc() != null)
            result.put(NAMED_SCHEMA_DOC_FIELD, namedSchema.doc());
        if (namedSchema instanceof EnumSchema enumSchema) {
            result.put(ENUM_SCHEMA_POSSIBLEVALUES_FIELD, enumSchema.symbols());
            if (enumSchema.defaultValue() != null)
                result.put(ENUM_SCHEMA_DEFAULTVALUE_FIELD, enumSchema.defaultValue());
        }
        if (namedSchema instanceof FixedSchema fixedSchema)
            result.put(FIXED_SCHEMA_SIZE_FIELD, fixedSchema.size());
        if (namedSchema instanceof StructSchema structSchema) {
            var fields = new ArrayList<Map<String, Object>>();
            result.put(STRUCT_SCHEMA_FIELDS_FIELD, fields);
            for (DataField field : structSchema.fields()) {
                fields.add(convertField(field));
            }
        }
    }

    private Object convertSchema(DataSchema schema) {
        if (schema == null) return null;
        return switch (schema.type()) {
            case ANY -> ANY_TYPE;
            case NULL -> NULL_TYPE;
            case BOOLEAN -> BOOLEAN_TYPE;
            case BYTE -> BYTE_TYPE;
            case SHORT -> SHORT_TYPE;
            case INTEGER -> INTEGER_TYPE;
            case LONG -> LONG_TYPE;
            case DOUBLE -> DOUBLE_TYPE;
            case FLOAT -> FLOAT_TYPE;
            case BYTES -> BYTES_TYPE;
            case FIXED -> FIXED_TYPE;
            case STRING -> STRING_TYPE;
            default -> fromDataSchema(schema);
        };
    }

    private Map<String, Object> convertField(DataField field) {
        var result = new HashMap<String, Object>();
        result.put(DATA_FIELD_NAME_FIELD, field.name());
        result.put(DATA_FIELD_DOC_FIELD, field.doc());
        result.put(DATA_FIELD_SCHEMA_FIELD, convertSchema(field.schema()));
        if (field.defaultValue() != null)
            encodeDefaultValue(result, DATA_FIELD_DEFAULT_VALUE_FIELD, field.defaultValue());
        result.put(DATA_FIELD_ORDER_FIELD, field.order().toString());
        return result;
    }

    private Object encodeDefaultValue(Map<String, Object> node, String fieldName, DataValue defaultValue) {
        if (defaultValue.value() == null) return node.put(fieldName, null);
        if (defaultValue.value() instanceof Byte value) return node.put(fieldName, value);
        if (defaultValue.value() instanceof Short value) return node.put(fieldName, value);
        if (defaultValue.value() instanceof Integer value) return node.put(fieldName, value);
        if (defaultValue.value() instanceof Long value) return node.put(fieldName, value);
        if (defaultValue.value() instanceof Double value) return node.put(fieldName, value);
        if (defaultValue.value() instanceof Float value) return node.put(fieldName, value);
        if (defaultValue.value() instanceof byte[] value) return node.put(fieldName, value);
        if (defaultValue.value() instanceof String value) return node.put(fieldName, value);
        throw new KSMLExecutionException("Can not encode default value of type: " + defaultValue.getClass().getSimpleName());
    }
}
