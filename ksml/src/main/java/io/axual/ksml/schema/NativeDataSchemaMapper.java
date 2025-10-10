package io.axual.ksml.schema;

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
import io.axual.ksml.data.schema.DataSchemaConstants;
import io.axual.ksml.data.schema.DataValue;
import io.axual.ksml.data.schema.EnumSchema;
import io.axual.ksml.data.schema.FixedSchema;
import io.axual.ksml.data.schema.ListSchema;
import io.axual.ksml.data.schema.MapSchema;
import io.axual.ksml.data.schema.NamedSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.schema.UnionSchema;
import io.axual.ksml.data.util.JsonNodeUtil;
import io.axual.ksml.data.util.ListUtil;
import io.axual.ksml.exception.ExecutionException;
import io.axual.ksml.parser.ParseNode;
import io.axual.ksml.schema.parser.DataSchemaDSL;
import io.axual.ksml.schema.parser.DataSchemaParser;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.axual.ksml.data.schema.DataSchema.ANY_SCHEMA;

public class NativeDataSchemaMapper implements DataSchemaMapper<Object> {
    private static final DataSchemaParser PARSER = new DataSchemaParser();

    @Override
    public DataSchema toDataSchema(String namespace, String name, Object value) {
        final var json = JsonNodeUtil.convertNativeToJsonNode(value);
        return PARSER.parse(ParseNode.fromRoot(json, "Schema"));
    }

    @Override
    public Object fromDataSchema(DataSchema schema) {
        if (schema instanceof UnionSchema unionSchema) {
            final var result = new ArrayList<>();
            for (final var memberSchema : unionSchema.members())
                result.add(convertMember(memberSchema));
            return result;
        }

        final var result = new LinkedHashMap<String, Object>();
        result.put(DataSchemaDSL.DATA_SCHEMA_TYPE_FIELD, getSchemaType(schema));
        if (schema instanceof ListSchema listSchema)
            result.put(DataSchemaDSL.LIST_SCHEMA_VALUES_FIELD, convertSchema(listSchema.valueSchema()));
        if (schema instanceof MapSchema mapSchema)
            result.put(DataSchemaDSL.MAP_SCHEMA_VALUES_FIELD, convertSchema(mapSchema.valueSchema()));
        if (schema instanceof NamedSchema namedSchema) writeNamedSchemaToMap(result, namedSchema);
        return result;
    }

    private String getSchemaType(DataSchema schema) {
        final var simpleType = getSimpleSchemaType(schema);
        if (simpleType != null) return simpleType;
        if (schema instanceof FixedSchema) return DataSchemaConstants.FIXED_TYPE;
        if (schema instanceof EnumSchema) return DataSchemaConstants.ENUM_TYPE;
        if (schema instanceof ListSchema) return DataSchemaConstants.LIST_TYPE;
        if (schema instanceof MapSchema) return DataSchemaConstants.MAP_TYPE;
        if (schema instanceof StructSchema) return DataSchemaConstants.STRUCT_TYPE;
        if (schema instanceof UnionSchema) return DataSchemaConstants.UNION_TYPE;
        throw new IllegalArgumentException("Unknown schema type: " + schema);
    }

    private void writeNamedSchemaToMap(Map<String, Object> result, NamedSchema namedSchema) {
        // Write out the named schema fields
        if (namedSchema.namespace() != null)
            result.put(DataSchemaDSL.NAMED_SCHEMA_NAMESPACE_FIELD, namedSchema.namespace());
        result.put(DataSchemaDSL.NAMED_SCHEMA_NAME_FIELD, namedSchema.name());
        if (namedSchema.hasDoc()) result.put(DataSchemaDSL.NAMED_SCHEMA_DOC_FIELD, namedSchema.doc());

        // Write out enum fields
        if (namedSchema instanceof EnumSchema enumSchema) {
            result.put(DataSchemaDSL.ENUM_SCHEMA_SYMBOLS_FIELD, convertSymbols(enumSchema.symbols()));
            if (enumSchema.defaultValue() != null)
                result.put(DataSchemaDSL.ENUM_SCHEMA_DEFAULT_VALUE_FIELD, enumSchema.defaultValue());
        }

        // Write out fixed fields
        if (namedSchema instanceof FixedSchema fixedSchema)
            result.put(DataSchemaDSL.FIXED_SCHEMA_SIZE_FIELD, fixedSchema.size());

        // Write out struct fields
        if (namedSchema instanceof StructSchema structSchema) {
            final var fields = new ArrayList<Map<String, Object>>();
            result.put(DataSchemaDSL.STRUCT_SCHEMA_FIELDS_FIELD, fields);
            for (final var field : structSchema.fields()) {
                fields.add(convertField(field));
            }
            result.put(DataSchemaDSL.STRUCT_SCHEMA_ADDITIONAL_FIELDS_ALLOWED_FIELD, structSchema.additionalFieldsAllowed());
            if (structSchema.additionalFieldsAllowed()) {
                final var schema = structSchema.additionalFieldsSchema() != null ? structSchema.additionalFieldsSchema() : ANY_SCHEMA;
                result.put(DataSchemaDSL.STRUCT_SCHEMA_ADDITIONAL_FIELDS_SCHEMA_FIELD, fromDataSchema(schema));
            }
        }
    }

    private List<Object> convertSymbols(List<EnumSchema.Symbol> symbols) {
        return ListUtil.map(symbols, this::convertSymbol);
    }

    private Map<String, Object> convertSymbol(EnumSchema.Symbol symbol) {
        final var result = new LinkedHashMap<String, Object>();
        result.put(DataSchemaDSL.ENUM_SYMBOL_NAME_FIELD, symbol.name());
        if (symbol.hasDoc()) result.put(DataSchemaDSL.ENUM_SYMBOL_DOC_FIELD, symbol.doc());
        result.put(DataSchemaDSL.ENUM_SYMBOL_TAG_FIELD, symbol.tag());
        return result;
    }

    private Object convertSchema(DataSchema schema) {
        if (schema == null) return null;
        final var simpleType = getSimpleSchemaType(schema);
        if (simpleType != null) return simpleType;
        return fromDataSchema(schema);
    }

    private String getSimpleSchemaType(DataSchema schema) {
        if (schema == ANY_SCHEMA) return DataSchemaConstants.ANY_TYPE;
        if (schema == DataSchema.NULL_SCHEMA) return DataSchemaConstants.NULL_TYPE;
        if (schema == DataSchema.BOOLEAN_SCHEMA) return DataSchemaConstants.BOOLEAN_TYPE;
        if (schema == DataSchema.BYTE_SCHEMA) return DataSchemaConstants.BYTE_TYPE;
        if (schema == DataSchema.SHORT_SCHEMA) return DataSchemaConstants.SHORT_TYPE;
        if (schema == DataSchema.INTEGER_SCHEMA) return DataSchemaConstants.INTEGER_TYPE;
        if (schema == DataSchema.LONG_SCHEMA) return DataSchemaConstants.LONG_TYPE;
        if (schema == DataSchema.DOUBLE_SCHEMA) return DataSchemaConstants.DOUBLE_TYPE;
        if (schema == DataSchema.FLOAT_SCHEMA) return DataSchemaConstants.FLOAT_TYPE;
        if (schema == DataSchema.BYTES_SCHEMA) return DataSchemaConstants.BYTES_TYPE;
        if (schema == DataSchema.STRING_SCHEMA) return DataSchemaConstants.STRING_TYPE;
        return null;
    }

    private Map<String, Object> convertField(DataField field) {
        final var result = new LinkedHashMap<String, Object>();
        result.put(DataSchemaDSL.DATA_FIELD_NAME_FIELD, field.name());
        result.put(DataSchemaDSL.DATA_FIELD_DOC_FIELD, field.doc());
        result.put(DataSchemaDSL.DATA_FIELD_REQUIRED_FIELD, field.required());
        result.put(DataSchemaDSL.DATA_FIELD_CONSTANT_FIELD, field.constant());
        result.put(DataSchemaDSL.DATA_FIELD_TAG_FIELD, field.tag());
        result.put(DataSchemaDSL.DATA_FIELD_SCHEMA_FIELD, convertSchema(field.schema()));
        if (field.defaultValue() != null) encodeDefaultValue(result, field.defaultValue());
        result.put(DataSchemaDSL.DATA_FIELD_ORDER_FIELD, field.order().toString());
        return result;
    }

    private Map<String, Object> convertMember(UnionSchema.Member member) {
        final var result = new LinkedHashMap<String, Object>();
        result.put(DataSchemaDSL.UNION_MEMBER_NAME_FIELD, member.name());
        result.put(DataSchemaDSL.UNION_MEMBER_SCHEMA_FIELD, convertSchema(member.schema()));
        result.put(DataSchemaDSL.UNION_MEMBER_TAG_FIELD, member.tag());
        return result;
    }

    private void encodeDefaultValue(Map<String, Object> node, DataValue defaultValue) {
        final var fieldName = DataSchemaDSL.DATA_FIELD_DEFAULT_VALUE_FIELD;
        switch (defaultValue.value()) {
            case null -> node.put(fieldName, null);
            case Byte value -> node.put(fieldName, value);
            case Short value -> node.put(fieldName, value);
            case Integer value -> node.put(fieldName, value);
            case Long value -> node.put(fieldName, value);
            case Double value -> node.put(fieldName, value);
            case Float value -> node.put(fieldName, value);
            case byte[] value -> node.put(fieldName, value);
            case String value -> node.put(fieldName, value);
            default ->
                    throw new ExecutionException("Can not encode default value of type: " + defaultValue.getClass().getSimpleName());
        }
    }
}
