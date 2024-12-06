package io.axual.ksml.data.mapper;

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

import io.axual.ksml.data.exception.ExecutionException;
import io.axual.ksml.data.notation.binary.JsonNodeNativeMapper;
import io.axual.ksml.data.parser.ParseNode;
import io.axual.ksml.data.parser.schema.DataSchemaDSL;
import io.axual.ksml.data.parser.schema.DataSchemaParser;
import io.axual.ksml.data.schema.*;
import io.axual.ksml.data.type.Symbol;
import io.axual.ksml.data.util.ListUtil;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class NativeDataSchemaMapper implements DataSchemaMapper<Object> {
    private static final DataSchemaParser PARSER = new DataSchemaParser();
    private static final JsonNodeNativeMapper JSON_NODE_MAPPER = new JsonNodeNativeMapper();

    @Override
    public DataSchema toDataSchema(String namespace, String name, Object value) {
        final var json = JSON_NODE_MAPPER.fromNative(value);
        return PARSER.parse(ParseNode.fromRoot(json, "Schema"));
    }

    @Override
    public Object fromDataSchema(DataSchema schema) {
        if (schema instanceof UnionSchema unionSchema) {
            final var result = new ArrayList<>();
            for (final var valueType : unionSchema.valueTypes())
                result.add(convertField(valueType));
            return result;
        }

        final var result = new LinkedHashMap<String, Object>();
        result.put(DataSchemaDSL.DATA_SCHEMA_TYPE_FIELD, schema.type().toString().toLowerCase());
        if (schema instanceof NamedSchema namedSchema)
            writeNamedSchemaToMap(result, namedSchema);
        if (schema instanceof ListSchema listSchema)
            result.put(DataSchemaDSL.LIST_SCHEMA_VALUES_FIELD, convertSchema(listSchema.valueSchema()));
        if (schema instanceof MapSchema mapSchema)
            result.put(DataSchemaDSL.MAP_SCHEMA_VALUES_FIELD, convertSchema(mapSchema.valueSchema()));
        return result;
    }

    private void writeNamedSchemaToMap(Map<String, Object> result, NamedSchema namedSchema) {
        if (namedSchema.namespace() != null)
            result.put(DataSchemaDSL.NAMED_SCHEMA_NAMESPACE_FIELD, namedSchema.namespace());
        result.put(DataSchemaDSL.NAMED_SCHEMA_NAME_FIELD, namedSchema.name());
        if (namedSchema.doc() != null)
            result.put(DataSchemaDSL.NAMED_SCHEMA_DOC_FIELD, namedSchema.doc());
        if (namedSchema instanceof EnumSchema enumSchema) {
            result.put(DataSchemaDSL.ENUM_SCHEMA_SYMBOLS_FIELD, convertSymbols(enumSchema.symbols()));
            if (enumSchema.defaultValue() != null)
                result.put(DataSchemaDSL.ENUM_SCHEMA_DEFAULT_VALUE_FIELD, enumSchema.defaultValue());
        }
        if (namedSchema instanceof FixedSchema fixedSchema)
            result.put(DataSchemaDSL.FIXED_SCHEMA_SIZE_FIELD, fixedSchema.size());
        if (namedSchema instanceof StructSchema structSchema) {
            final var fields = new ArrayList<Map<String, Object>>();
            result.put(DataSchemaDSL.STRUCT_SCHEMA_FIELDS_FIELD, fields);
            for (final var field : structSchema.fields()) {
                fields.add(convertField(field));
            }
        }
    }

    private List<Object> convertSymbols(List<Symbol> symbols) {
        return ListUtil.map(symbols, this::convertSymbol);
    }

    private Map<String, Object> convertSymbol(Symbol symbol) {
        final var result = new LinkedHashMap<String, Object>();
        result.put(DataSchemaDSL.ENUM_SYMBOL_NAME_FIELD, symbol.name());
        result.put(DataSchemaDSL.ENUM_SYMBOL_DOC_FIELD, symbol.doc());
        result.put(DataSchemaDSL.ENUM_SYMBOL_INDEX_FIELD, symbol.index());
        return result;
    }

    private Object convertSchema(DataSchema schema) {
        if (schema == null) return null;
        return switch (schema.type()) {
            case ANY -> DataSchemaDSL.ANY_TYPE;
            case NULL -> DataSchemaDSL.NULL_TYPE;
            case BOOLEAN -> DataSchemaDSL.BOOLEAN_TYPE;
            case BYTE -> DataSchemaDSL.BYTE_TYPE;
            case SHORT -> DataSchemaDSL.SHORT_TYPE;
            case INTEGER -> DataSchemaDSL.INTEGER_TYPE;
            case LONG -> DataSchemaDSL.LONG_TYPE;
            case DOUBLE -> DataSchemaDSL.DOUBLE_TYPE;
            case FLOAT -> DataSchemaDSL.FLOAT_TYPE;
            case BYTES -> DataSchemaDSL.BYTES_TYPE;
            case FIXED -> DataSchemaDSL.FIXED_TYPE;
            case STRING -> DataSchemaDSL.STRING_TYPE;
            default -> fromDataSchema(schema);
        };
    }

    private Map<String, Object> convertField(DataField field) {
        final var result = new LinkedHashMap<String, Object>();
        result.put(DataSchemaDSL.DATA_FIELD_NAME_FIELD, field.name());
        result.put(DataSchemaDSL.DATA_FIELD_DOC_FIELD, field.doc());
        result.put(DataSchemaDSL.DATA_FIELD_REQUIRED_FIELD, field.required());
        result.put(DataSchemaDSL.DATA_FIELD_CONSTANT_FIELD, field.constant());
        result.put(DataSchemaDSL.DATA_FIELD_INDEX_FIELD, field.index());
        result.put(DataSchemaDSL.DATA_FIELD_SCHEMA_FIELD, convertSchema(field.schema()));
        if (field.defaultValue() != null)
            encodeDefaultValue(result, DataSchemaDSL.DATA_FIELD_DEFAULT_VALUE_FIELD, field.defaultValue());
        result.put(DataSchemaDSL.DATA_FIELD_ORDER_FIELD, field.order().toString());
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
        throw new ExecutionException("Can not encode default value of type: " + defaultValue.getClass().getSimpleName());
    }
}
