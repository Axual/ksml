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
import io.axual.ksml.schema.DataField;
import io.axual.ksml.schema.DataSchema;
import io.axual.ksml.schema.DataValue;
import io.axual.ksml.schema.EnumSchema;
import io.axual.ksml.schema.FixedSchema;
import io.axual.ksml.schema.ListSchema;
import io.axual.ksml.schema.MapSchema;
import io.axual.ksml.schema.NamedSchema;
import io.axual.ksml.schema.StructSchema;
import io.axual.ksml.schema.UnionSchema;
import io.axual.ksml.schema.parser.DataSchemaParser;

import static io.axual.ksml.schema.structure.DataSchemaConstants.DATAFIELD_DEFAULT_VALUE_FIELD;
import static io.axual.ksml.schema.structure.DataSchemaConstants.DATAFIELD_DOC_FIELD;
import static io.axual.ksml.schema.structure.DataSchemaConstants.DATAFIELD_NAME_FIELD;
import static io.axual.ksml.schema.structure.DataSchemaConstants.DATAFIELD_ORDER_FIELD;
import static io.axual.ksml.schema.structure.DataSchemaConstants.DATAFIELD_SCHEMA_FIELD;
import static io.axual.ksml.schema.structure.DataSchemaConstants.DATASCHEMA_TYPE_FIELD;
import static io.axual.ksml.schema.structure.DataSchemaConstants.ENUMSCHEMA_DEFAULTVALUE_FIELD;
import static io.axual.ksml.schema.structure.DataSchemaConstants.ENUMSCHEMA_POSSIBLEVALUES_FIELD;
import static io.axual.ksml.schema.structure.DataSchemaConstants.FIXEDSCHEMA_SIZE_FIELD;
import static io.axual.ksml.schema.structure.DataSchemaConstants.LISTSCHEMA_VALUES_FIELD;
import static io.axual.ksml.schema.structure.DataSchemaConstants.MAPSCHEMA_VALUES_FIELD;
import static io.axual.ksml.schema.structure.DataSchemaConstants.NAMEDSCHEMA_DOC_FIELD;
import static io.axual.ksml.schema.structure.DataSchemaConstants.NAMEDSCHEMA_NAMESPACE_FIELD;
import static io.axual.ksml.schema.structure.DataSchemaConstants.NAMEDSCHEMA_NAME_FIELD;
import static io.axual.ksml.schema.structure.DataSchemaConstants.STRUCTSCHEMA_FIELDS_FIELD;

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
            for (DataSchema possibleSchema : unionSchema.possibleSchema())
                result.add(convertSchema(possibleSchema));
            return result;
        }

        var result = new HashMap<String, Object>();
        result.put(DATASCHEMA_TYPE_FIELD, schema.type().toString().toLowerCase());
        if (schema instanceof NamedSchema namedSchema)
            writeNamedSchemaToMap(result, namedSchema);
        if (schema instanceof ListSchema listSchema)
            result.put(LISTSCHEMA_VALUES_FIELD, convertSchema(listSchema.valueType()));
        if (schema instanceof MapSchema mapSchema)
            result.put(MAPSCHEMA_VALUES_FIELD, convertSchema(mapSchema.valueSchema()));
        return result;
    }

    private void writeNamedSchemaToMap(Map<String, Object> result, NamedSchema namedSchema) {
        if (namedSchema.namespace() != null)
            result.put(NAMEDSCHEMA_NAMESPACE_FIELD, namedSchema.namespace());
        result.put(NAMEDSCHEMA_NAME_FIELD, namedSchema.name());
        if (namedSchema.doc() != null)
            result.put(NAMEDSCHEMA_DOC_FIELD, namedSchema.doc());
        if (namedSchema instanceof EnumSchema enumSchema) {
            result.put(ENUMSCHEMA_POSSIBLEVALUES_FIELD, enumSchema.symbols());
            if (enumSchema.defaultValue() != null)
                result.put(ENUMSCHEMA_DEFAULTVALUE_FIELD, enumSchema.defaultValue());
        }
        if (namedSchema instanceof FixedSchema fixedSchema)
            result.put(FIXEDSCHEMA_SIZE_FIELD, fixedSchema.size());
        if (namedSchema instanceof StructSchema structSchema) {
            var fields = new ArrayList<Map<String, Object>>();
            result.put(STRUCTSCHEMA_FIELDS_FIELD, fields);
            for (DataField field : structSchema.fields()) {
                fields.add(convertField(field));
            }
        }
    }

    private Object convertSchema(DataSchema schema) {
        return switch (schema.type()) {
            case NULL, BYTE, SHORT, INTEGER, LONG, DOUBLE, FLOAT, BYTES, STRING -> schema.type().toString().toLowerCase();
            default -> fromDataSchema(schema);
        };
    }

    private Map<String, Object> convertField(DataField field) {
        var result = new HashMap<String, Object>();
        result.put(DATAFIELD_NAME_FIELD, field.name());
        result.put(DATAFIELD_DOC_FIELD, field.doc());
        result.put(DATAFIELD_SCHEMA_FIELD, convertSchema(field.schema()));
        if (field.defaultValue() != null && field.defaultValue().value() != null)
            encodeDefaultValue(result, DATAFIELD_DEFAULT_VALUE_FIELD, field.defaultValue());
        result.put(DATAFIELD_ORDER_FIELD, field.order().toString());
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
