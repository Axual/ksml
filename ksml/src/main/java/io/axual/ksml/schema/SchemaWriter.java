package io.axual.ksml.schema;

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


import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.StringWriter;
import java.io.Writer;

import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.parser.YamlNode;
import io.axual.ksml.schema.parser.DataSchemaParser;

public class SchemaWriter {
    public static final String DATASCHEMA_TYPE_FIELD = "type";
    public static final String NAMEDSCHEMA_NAMESPACE_FIELD = "namespace";
    public static final String NAMEDSCHEMA_NAME_FIELD = "name";
    public static final String NAMEDSCHEMA_DOC_FIELD = "doc";
    public static final String ENUMSCHEMA_POSSIBLEVALUES_FIELD = "symbols";
    public static final String ENUMSCHEMA_DEFAULTVALUE_FIELD = "defaultValue";
    public static final String FIXEDSCHEMA_SIZE_FIELD = "size";
    public static final String LISTSCHEMA_VALUES_FIELD = "items";
    public static final String MAPSCHEMA_VALUES_FIELD = "values";
    public static final String STRUCTSCHEMA_FIELDS_FIELD = "fields";
    public static final String DATAFIELD_NAME_FIELD = "name";
    public static final String DATAFIELD_SCHEMA_FIELD = "type";
    public static final String DATAFIELD_DOC_FIELD = "doc";
    public static final String DATAFIELD_DEFAULT_VALUE_FIELD = "defaultValue";
    public static final String DATAFIELD_ORDER_FIELD = "order";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private SchemaWriter() {
    }

    public static String writeDataSchemaAsString(DataSchema schema) {
        JsonNode node = writeDataSchemaAsObject(schema);
        JsonFactory jsonFactory = new JsonFactory();
        Writer writer = new StringWriter();
        try {
            JsonGenerator jsonGenerator = jsonFactory.createGenerator(writer);
            MAPPER.writeTree(jsonGenerator, node);
        } catch (Exception e) {
            throw new KSMLExecutionException("Could not write schema as string");
        }
        return writer.toString();
    }

    public static JsonNode writeDataSchemaAsObject(DataSchema schema) {
        if (schema instanceof UnionSchema unionSchema) {
            var result = MAPPER.createArrayNode();
            for (DataSchema possibleSchema : unionSchema.possibleSchema()) {
                addDataSchemaToArray(result, possibleSchema);
            }
            return result;
        }

        var result = MAPPER.createObjectNode();
        result.put(DATASCHEMA_TYPE_FIELD, schema.type().toString().toLowerCase());
        if (schema instanceof NamedSchema namedSchema) {
            writeNamedSchemaToObject(result, namedSchema);
        }
        if (schema instanceof ListSchema listSchema) {
            writeDataSchemaAsField(result, LISTSCHEMA_VALUES_FIELD, listSchema.valueType());
        }
        if (schema instanceof MapSchema mapSchema) {
            writeDataSchemaAsField(result, MAPSCHEMA_VALUES_FIELD, mapSchema.valueSchema());
        }
        return result;
    }

    private static void writeNamedSchemaToObject(ObjectNode node, NamedSchema schema) {
        if (schema.namespace() != null)
            node.put(NAMEDSCHEMA_NAMESPACE_FIELD, schema.namespace());
        node.put(NAMEDSCHEMA_NAME_FIELD, schema.name());
        if (schema.doc() != null)
            node.put(NAMEDSCHEMA_DOC_FIELD, schema.doc());
        if (schema instanceof EnumSchema enumSchema) {
            var symbols = node.putArray(ENUMSCHEMA_POSSIBLEVALUES_FIELD);
            for (String possibleValue : enumSchema.possibleValues()) {
                symbols.add(possibleValue);
            }
            if (enumSchema.defaultValue() != null)
                node.put(ENUMSCHEMA_DEFAULTVALUE_FIELD, enumSchema.defaultValue());
        }
        if (schema instanceof FixedSchema fixedSchema) {
            node.put(FIXEDSCHEMA_SIZE_FIELD, fixedSchema.size());
        }
        if (schema instanceof StructSchema structSchema) {
            var fields = node.putArray(STRUCTSCHEMA_FIELDS_FIELD);
            for (DataField field : structSchema.fields()) {
                fields.add(writeField(field));
            }
        }
    }

    private static Object writeDataSchemaBriefly(DataSchema schema) {
        return switch (schema.type()) {
            case NULL, BYTE, SHORT, INTEGER, LONG, DOUBLE, FLOAT, BYTES, STRING -> schema.type().toString().toLowerCase();
            default -> writeDataSchemaAsObject(schema);
        };
    }

    private static ObjectNode writeField(DataField field) {
        var result = MAPPER.createObjectNode()
                .put(DATAFIELD_NAME_FIELD, field.name())
                .put(DATAFIELD_DOC_FIELD, field.doc());
        writeDataSchemaAsField(result, DATAFIELD_SCHEMA_FIELD, field.schema());
        if (field.defaultValue() != null)
            encodeDefaultValue(result, DATAFIELD_DEFAULT_VALUE_FIELD, field.defaultValue());
        return result.put(DATAFIELD_ORDER_FIELD, field.order().toString());
    }

    private static void addDataSchemaToArray(ArrayNode node, DataSchema schema) {
        if (schema != null) {
            var encodedSchema = writeDataSchemaBriefly(schema);
            if (encodedSchema instanceof String encodedSchemaStr)
                node.add(encodedSchemaStr);
            else
                node.add((JsonNode) encodedSchema);
        } else {
            node.add("null");
        }
    }

    private static void writeDataSchemaAsField(ObjectNode node, String fieldName, DataSchema schema) {
        var encodedSchema = writeDataSchemaBriefly(schema);
        if (encodedSchema instanceof String encodedSchemaStr)
            node.put(fieldName, encodedSchemaStr);
        else
            node.set(fieldName, (JsonNode) encodedSchema);
    }

    private static ObjectNode encodeDefaultValue(ObjectNode node, String fieldName, DataValue defaultValue) {
        if (defaultValue.value() == null) return node.set(fieldName, null);
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

    public static DataSchema read(String schema) {
        try {
            var root = MAPPER.readTree(schema);
            return new DataSchemaParser().parse(YamlNode.fromRoot(root, "schema"));
        } catch (JsonProcessingException e) {
            // Ignore errors and return null schema
            return null;
        }
    }
}
