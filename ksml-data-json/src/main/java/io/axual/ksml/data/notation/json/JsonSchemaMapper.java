package io.axual.ksml.data.notation.json;

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
import io.axual.ksml.data.notation.ReferenceResolver;
import io.axual.ksml.data.object.DataBoolean;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.schema.DataField;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.DataValue;
import io.axual.ksml.data.schema.EnumSchema;
import io.axual.ksml.data.schema.ListSchema;
import io.axual.ksml.data.schema.MapSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.schema.UnionSchema;
import io.axual.ksml.data.type.Symbol;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static io.axual.ksml.data.schema.DataField.NO_TAG;
import static io.axual.ksml.data.schema.DataSchema.ANY_SCHEMA;

/**
 * Mapper between JSON Schema text and KSML DataSchema.
 *
 * <p>Responsibilities:</p>
 * <ul>
 *   <li>Parse a JSON Schema document (as String or DataStruct) into {@link DataSchema}, producing
 *       a {@link StructSchema} with fields, required markers, additionalProperties, lists, unions, and enums.</li>
 *   <li>Generate a JSON Schema (as a DataStruct or compact String) from a {@link DataSchema}, including
 *       definitions when fields reference reusable structures.</li>
 * </ul>
 *
 * <p>Design notes:</p>
 * <ul>
 *   <li>Parsing happens via {@link JsonDataObjectMapper} to get a DataStruct view of the input JSON.</li>
 *   <li>A simple ReferenceResolver supports local $ref paths within the same JSON document.</li>
 *   <li>Only a subset of JSON Schema keywords is handled as required by KSML.</li>
 * </ul>
 */
public class JsonSchemaMapper implements DataSchemaMapper<String> {
    private static final String TITLE_NAME = "title";
    private static final String DESCRIPTION_NAME = "description";
    private static final String TYPE_NAME = "type";
    private static final String PROPERTIES_NAME = "properties";
    private static final String PATTERN_PROPERTIES_NAME = "patternProperties";
    private static final String ALL_PROPERTIES_REGEX = "^[a-zA-Z0-9_]+$";
    private static final String ITEMS_NAME = "items";
    private static final String REQUIRED_NAME = "required";
    private static final String ADDITIONAL_PROPERTIES = "additionalProperties";
    private static final String DEFINITIONS_NAME = "$defs";
    private static final String REF_NAME = "$ref";
    private static final String ANY_OF_NAME = "anyOf";
    private static final String ENUM_NAME = "enum";
    private static final String ARRAY_TYPE = "array";
    private static final String BOOLEAN_TYPE = "boolean";
    private static final String NULL_TYPE = "null";
    private static final String INTEGER_TYPE = "integer";
    private static final String NUMBER_TYPE = "number";
    private static final String OBJECT_TYPE = "object";
    private static final String STRING_TYPE = "string";
    private final JsonDataObjectMapper mapper;

    /**
     * Creates a mapper for JSON Schema <-> DataSchema conversions.
     *
     * @param prettyPrint when true, JSON emitted by {@link #fromDataSchema(DataSchema)} is formatted
     */
    public JsonSchemaMapper(boolean prettyPrint) {
        mapper = new JsonDataObjectMapper(prettyPrint);
    }

    /**
     * Parses a JSON Schema document (string) into a KSML {@link DataSchema}.
     *
     * <p>The JSON is first converted to a DataStruct via {@link JsonDataObjectMapper}, after which
     * this mapper interprets JSON Schema keywords and builds a {@link StructSchema} or related types.</p>
     *
     * @param namespace the target namespace for the resulting StructSchema
     * @param name      the default schema name used when no explicit title is present
     * @param value     the JSON Schema document as a String
     * @return the parsed DataSchema
     * @throws IllegalArgumentException when the input does not parse into a DataStruct
     */
    @Override
    public DataSchema toDataSchema(String namespace, String name, String value) {
        // Convert JSON to internal DataObject format
        var schema = mapper.toDataObject(value);
        if (!(schema instanceof DataStruct schemaStruct)) {
            throw new IllegalArgumentException("Could not parse JSON Schema as a data object");
        }

        // Set up a reference resolver to find referenced types during parsing
        final ReferenceResolver<DataStruct> referenceResolver = path -> {
            final var parts = path.split("/");
            if (parts.length <= 1 || !parts[0].equals("#")) return null;
            var result = schemaStruct;
            for (var i = 1; i < parts.length; i++) {
                final var sub = result.get(parts[i]);
                if (!(sub instanceof DataStruct subStruct)) return null;
                result = subStruct;
            }
            return result;
        };

        // Now interpret the DataStruct representation as a StructSchema
        return toDataSchema(namespace, name, schemaStruct, referenceResolver);
    }

    /**
     * Internal parse step that interprets a JSON Schema represented as a DataStruct.
     *
     * @param namespace         target namespace for the resulting StructSchema
     * @param name              default name when title is absent
     * @param schema            the JSON Schema as DataStruct
     * @param referenceResolver resolves local $ref paths within the same document
     * @return a DataSchema equivalent
     */
    private DataSchema toDataSchema(String namespace, String name, DataStruct schema, ReferenceResolver<DataStruct> referenceResolver) {
        final var title = schema.getAsString(TITLE_NAME);
        final var doc = schema.getAsString(DESCRIPTION_NAME);

        final var requiredProperties = new TreeSet<String>();
        List<DataField> fields = null;
        final var reqProps = schema.get(REQUIRED_NAME);
        if (reqProps instanceof DataList reqPropList) {
            for (var reqProp : reqPropList) {
                requiredProperties.add(reqProp.toString());
            }
        }

        var additionalPropertiesSchema = ANY_SCHEMA;
        var additionalPropertiesAllowed = true;
        final var additionalProperties = schema.get(ADDITIONAL_PROPERTIES);
        if (additionalProperties instanceof DataBoolean dataBoolean) {
            additionalPropertiesAllowed = dataBoolean.value();
            if (!additionalPropertiesAllowed) {
                additionalPropertiesSchema = null;
            }
        } else if (additionalProperties instanceof DataStruct structData) {
            additionalPropertiesSchema = convertType(structData, referenceResolver);
        }

        final var properties = schema.get(PROPERTIES_NAME);
        if (properties instanceof DataStruct propertiesStruct)
            fields = convertFields(propertiesStruct, requiredProperties, referenceResolver);
        return StructSchema.builder()
                .namespace(namespace)
                .name(title != null ? title.value() : name)
                .doc(doc != null ? doc.value() : null)
                .fields(fields)
                .additionalFieldsAllowed(additionalPropertiesAllowed)
                .additionalFieldsSchema(additionalPropertiesSchema)
                .build();
    }

    /**
     * Converts JSON Schema 'properties' into a list of KSML DataFields.
     *
     * @param properties         a DataStruct mapping property names to their JSON Schema specs
     * @param requiredProperties set of required property names
     * @param referenceResolver  resolver for local $ref targets
     * @return list of DataField instances in no particular order
     */
    private List<DataField> convertFields(DataStruct properties, Set<String> requiredProperties, ReferenceResolver<DataStruct> referenceResolver) {
        var result = new ArrayList<DataField>();
        for (var entry : properties.entrySet()) {
            var name = entry.getKey();
            var spec = entry.getValue();
            if (spec instanceof DataStruct specStruct) {
                var doc = specStruct.getAsString(DESCRIPTION_NAME);
                var field = new DataField(name, convertType(specStruct, referenceResolver), doc != null ? doc.value() : null, NO_TAG, requiredProperties.contains(name));
                result.add(field);
            }
        }
        return result;
    }

    /**
     * Converts a JSON Schema type specification (as DataStruct) into a KSML {@link DataSchema}.
     * Supports anyOf (unions), enum, primitive types, arrays (items) and objects (with optional $ref).
     */
    private DataSchema convertType(DataStruct specStruct, ReferenceResolver<DataStruct> referenceResolver) {
        final var anyOf = specStruct.get(ANY_OF_NAME);
        if (anyOf instanceof DataList anyOfList) {
            final var memberSchemas = new DataField[anyOfList.size()];
            for (var index = 0; index < anyOfList.size(); index++) {
                final var anyOfMember = anyOfList.get(index);
                if (anyOfMember instanceof DataStruct anyOfMemberStruct)
                    memberSchemas[index] = new DataField(null, convertType(anyOfMemberStruct, referenceResolver));
            }
            return new UnionSchema(memberSchemas);
        }

        final var enumSymbols = specStruct.get(ENUM_NAME);
        if (enumSymbols instanceof DataList enumList) {
            final var symbols = new ArrayList<Symbol>();
            enumList.forEach(enumSymbol -> {
                if (enumSymbol instanceof DataString enumSymbolStr) symbols.add(new Symbol(enumSymbolStr.value()));
            });
            return new EnumSchema(null, null, null, symbols);
        }

        final var type = specStruct.getAsString(TYPE_NAME);
        if (type == null || type.isEmpty()) return ANY_SCHEMA;
        return switch (type.value()) {
            case NULL_TYPE -> DataSchema.NULL_SCHEMA;
            case BOOLEAN_TYPE -> DataSchema.BOOLEAN_SCHEMA;
            case INTEGER_TYPE -> DataSchema.LONG_SCHEMA;
            case NUMBER_TYPE -> DataSchema.DOUBLE_SCHEMA;
            case STRING_TYPE -> DataSchema.STRING_SCHEMA;
            case ARRAY_TYPE -> toListSchema(specStruct, referenceResolver);
            case OBJECT_TYPE -> {
                final var ref = specStruct.getAsString(REF_NAME);
                if (ref instanceof DataString refString) {
                    final var refType = referenceResolver.get(refString.value());
                    if (refType != null) yield toDataSchema(null, null, refType, referenceResolver);
                }
                yield toDataSchema(null, null, specStruct, referenceResolver);
            }
            default -> ANY_SCHEMA;
        };
    }

    /**
     * Converts a JSON Schema 'array' specification into a {@link ListSchema}.
     * Uses 'items' to define the value schema; defaults to ANY when missing.
     */
    private ListSchema toListSchema(DataStruct spec, ReferenceResolver<DataStruct> referenceResolver) {
        var items = spec.get(ITEMS_NAME);
        if (items instanceof DataStruct itemsStruct) {
            var valueSchema = convertType(itemsStruct, referenceResolver);
            return new ListSchema(valueSchema);
        }
        return new ListSchema(ANY_SCHEMA);
    }

    /**
     * Generates a JSON Schema document (as String) from a KSML {@link DataSchema}.
     * Only {@link StructSchema} is emitted; other schema kinds return null.
     */
    @Override
    public String fromDataSchema(DataSchema schema) {
        if (schema instanceof StructSchema structSchema) {
            final var result = fromDataSchema(structSchema);
            // First translate the schema into DataObjects
            // The use the mapper to convert it into JSON
            return mapper.fromDataObject(result);
        }
        return null;
    }

    /**
     * Minimal callback used to collect/declare reusable schema definitions when generating JSON Schema.
     */
    private interface DefinitionLibrary {
        /**
         * Registers a definition by name if absent, with its corresponding JSON Schema (as DataStruct).
         */
        void put(String name, DataStruct schema);
    }

    /**
     * Translates a {@link StructSchema} into a JSON Schema represented as DataStruct.
     * Populates $defs with referenced structures if any are encountered.
     */
    private DataStruct fromDataSchema(StructSchema structSchema) {
        final var definitions = new DataStruct();
        final var result = fromDataSchema(structSchema, definitions::putIfAbsent);
        if (definitions.size() > 0) {
            result.put(DEFINITIONS_NAME, definitions);
        }
        return result;
    }

    /**
     * Translates a {@link StructSchema} into a JSON Schema DataStruct, using a definition library
     * to collect reusable structures by name and reference them via $ref.
     */
    private DataStruct fromDataSchema(StructSchema structSchema, DefinitionLibrary definitions) {
        final var result = new DataStruct();
        final var title = structSchema.name();
        if (title != null) result.put(TITLE_NAME, new DataString(title));
        if (structSchema.hasDoc()) result.put(DESCRIPTION_NAME, new DataString(structSchema.doc()));
        result.put(TYPE_NAME, new DataString(OBJECT_TYPE));
        final var requiredProperties = new TreeSet<String>();
        final var properties = new DataStruct();
        for (var field : structSchema.fields()) {
            properties.put(field.name(), fromDataSchema(field, definitions));
            if (field.required()) requiredProperties.add(field.name());
        }
        if (!requiredProperties.isEmpty()) {
            // Copy into a list now, ensuring alphabetical ordering of property names
            final var reqProps = new DataList(DataString.DATATYPE);
            requiredProperties.forEach(s -> reqProps.add(new DataString(s)));
            result.put(REQUIRED_NAME, reqProps);
        }
        if (properties.size() > 0) result.put(PROPERTIES_NAME, properties);
        if (structSchema.additionalFieldsAllowed()) {
            if (ANY_SCHEMA.equals(structSchema.additionalFieldsSchema())) {
                result.put(ADDITIONAL_PROPERTIES, new DataBoolean(true));
            } else {
                result.put(ADDITIONAL_PROPERTIES, fromDataSchema(structSchema.additionalFieldsSchema(), definitions));
            }
        } else {
            result.put(ADDITIONAL_PROPERTIES, new DataBoolean(false));
        }
        return result;
    }

    private DataStruct fromDataSchema(DataField field, DefinitionLibrary definitions) {
        final var result = new DataStruct();
        final var doc = field.doc();
        if (doc != null) result.put(DESCRIPTION_NAME, new DataString(doc));
        convertType(field.schema(), field.constant(), field.defaultValue(), result, definitions);
        return result;
    }

    private DataStruct fromDataSchema(DataSchema schema, DefinitionLibrary definitions) {
        final var result = new DataStruct();
        convertType(schema, false, null, result, definitions);
        return result;
    }

    private void convertType(DataSchema schema, boolean constant, DataValue defaultValue, DataStruct target, DefinitionLibrary definitions) {
        if (schema == DataSchema.NULL_SCHEMA) target.put(TYPE_NAME, new DataString(NULL_TYPE));
        if (schema == DataSchema.BOOLEAN_SCHEMA) target.put(TYPE_NAME, new DataString(BOOLEAN_TYPE));
        if (schema == DataSchema.BYTE_SCHEMA || schema == DataSchema.SHORT_SCHEMA || schema == DataSchema.INTEGER_SCHEMA || schema == DataSchema.LONG_SCHEMA)
            target.put(TYPE_NAME, new DataString(INTEGER_TYPE));
        if (schema == DataSchema.FLOAT_SCHEMA || schema == DataSchema.DOUBLE_SCHEMA)
            target.put(TYPE_NAME, new DataString(NUMBER_TYPE));
        if (schema == DataSchema.STRING_SCHEMA) {
            if (constant && defaultValue != null && defaultValue.value() != null) {
                var enumList = new DataList();
                enumList.add(new DataString(defaultValue.value().toString()));
                target.put(ENUM_NAME, enumList);
            } else {
                target.put(TYPE_NAME, new DataString(STRING_TYPE));
            }
        }
        if (schema instanceof EnumSchema enumSchema) {
            var enumList = new DataList();
            enumSchema.symbols().forEach(symbol -> enumList.add(new DataString(symbol.name())));
            target.put(ENUM_NAME, enumList);
        }
        if (schema instanceof ListSchema listSchema) {
            target.put(TYPE_NAME, new DataString(ARRAY_TYPE));
            final var subStruct = new DataStruct();
            convertType(listSchema.valueSchema(), false, null, subStruct, definitions);
            if (subStruct.size() > 0) {
                // only add if the list values exist
                target.put(ITEMS_NAME, subStruct);
            }
        }
        if (schema instanceof MapSchema mapSchema) {
            target.put(TYPE_NAME, new DataString(OBJECT_TYPE));
            if (ANY_SCHEMA.equals(mapSchema.valueSchema())) {
                target.put(ADDITIONAL_PROPERTIES, new DataBoolean(true));
            } else {
                final var additionalPatternSubStruct = new DataStruct();
                target.put(ADDITIONAL_PROPERTIES, additionalPatternSubStruct);
                convertType(mapSchema.valueSchema(), false, null, additionalPatternSubStruct, definitions);
            }
        }
        if (schema instanceof StructSchema structSchema) {
            final var name = structSchema.name();
            definitions.put(name, fromDataSchema(structSchema, definitions));
            target.put(TYPE_NAME, new DataString(OBJECT_TYPE));
            target.put(REF_NAME, new DataString("#/" + DEFINITIONS_NAME + "/" + name));
        }
        if (schema instanceof UnionSchema unionSchema) {
            // Convert to an array of value types
            final var memberSchemas = new DataList();
            for (var memberSchema : unionSchema.memberSchemas()) {
                final var typeStruct = new DataStruct();
                convertType(memberSchema.schema(), false, null, typeStruct, definitions);
                memberSchemas.add(typeStruct);
            }
            target.put(ANY_OF_NAME, memberSchemas);
        }
    }
}
