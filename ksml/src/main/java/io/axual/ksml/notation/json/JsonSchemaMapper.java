package io.axual.ksml.notation.json;

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
import io.axual.ksml.data.object.DataBoolean;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.schema.AnySchema;
import io.axual.ksml.data.schema.DataField;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.DataValue;
import io.axual.ksml.data.schema.ListSchema;
import io.axual.ksml.data.schema.MapSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.schema.UnionSchema;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class JsonSchemaMapper implements DataSchemaMapper<String> {
    private static final JsonDataObjectMapper MAPPER = new JsonDataObjectMapper();
    private static final String TITLE_NAME = "title";
    private static final String DESCRIPTION_NAME = "description";
    private static final String TYPE_NAME = "type";
    private static final String PROPERTIES_NAME = "properties";
    private static final String PATTERN_PROPERTIES_NAME = "patternProperties";
    private static final String ALL_PROPERTIES_REGEX = "^[a-zA-Z0-9_]+$";
    private static final String ITEMS_NAME = "items";
    private static final String REQUIRED_NAME = "required";
    private static final String ADDITIONAL_PROPERTIES = "additionalProperties";
    private static final String DEFINITIONS_NAME = "definitions";
    private static final String REF_NAME = "$ref";
    private static final String ANY_OF_NAME = "anyOf";
    private static final String ENUM_NAME = "enum";

    @Override
    public DataSchema toDataSchema(String namespace, String name, String value) {
        // Convert JSON to internal DataObject format
        var schema = MAPPER.toDataObject(value);
        if (schema instanceof DataStruct schemaStruct) {
            return toDataSchema(namespace,name,schemaStruct);
        }
        return null;
    }

    private DataSchema toDataSchema(String namespace, String name, DataStruct schema) {
        final var title = schema.getAsString(TITLE_NAME);
        final var doc = schema.getAsString(DESCRIPTION_NAME);

        final var requiredProperties = new HashSet<String>();
        List<DataField> fields = null;
        final var reqProps = schema.get(REQUIRED_NAME);
        if (reqProps instanceof DataList reqPropList) {
            for (var reqProp : reqPropList) {
                requiredProperties.add(reqProp.toString());
            }
        }

        final var properties = schema.get(PROPERTIES_NAME);
        if (properties instanceof DataStruct propertiesStruct)
            fields = convertFields(propertiesStruct, requiredProperties);
        return new StructSchema(namespace, title != null ? title.value() : name, doc != null ? doc.value() : null, fields);
    }

    private List<DataField> convertFields(DataStruct properties, Set<String> requiredProperties) {
        var result = new ArrayList<DataField>();
        for (var entry : properties.entrySet()) {
            var name = entry.getKey();
            var spec = entry.getValue();
            if (spec instanceof DataStruct specStruct) {
                var doc = specStruct.getAsString(DESCRIPTION_NAME);
                var field = new DataField(name, convertType(specStruct), doc != null ? doc.value() : null, requiredProperties.contains(name));
                result.add(field);
            }
        }
        return result;
    }

    private DataSchema convertType(DataStruct specStruct) {
        var type = specStruct.getAsString(TYPE_NAME);
        if (type != null) {
            switch (type.value()) {
                case "null":
                    return DataSchema.create(DataSchema.Type.NULL);
                case "boolean":
                    return DataSchema.create(DataSchema.Type.BOOLEAN);
                case "number":
                    return DataSchema.create(DataSchema.Type.LONG);
                case "array":
                    return toListSchema(specStruct);
                case "object":
                    return toDataSchema(specStruct);
                case "string":
                    return DataSchema.create(DataSchema.Type.STRING);
            }
        }
        return AnySchema.INSTANCE;
    }

    private ListSchema toListSchema(DataStruct spec) {
        var items = spec.get(ITEMS_NAME);
        if (items instanceof DataStruct itemsStruct) {
            var valueSchema = convertType(itemsStruct);
            return new ListSchema(valueSchema);
        }
        return new ListSchema(AnySchema.INSTANCE);
    }

    @Override
    public String fromDataSchema(DataSchema schema) {
        if (schema instanceof StructSchema structSchema) {
            final var result = fromDataSchema(structSchema);
            // First translate the schema into DataObjects
            // The use the mapper to convert it into JSON
            return MAPPER.fromDataObject(result);
        }
        return null;
    }

    public DataStruct fromDataSchema(StructSchema structSchema) {
        final var definitions = new DataStruct();
        final var result = fromDataSchema(structSchema, definitions);
        if (definitions.size() > 0) {
            result.put(DEFINITIONS_NAME, definitions);
        }
        return result;
    }

    public DataStruct fromDataSchema(StructSchema structSchema, DataStruct definitions) {
        final var result = new DataStruct();
        final var title = structSchema.name();
        final var doc = structSchema.doc();
        if (title != null) result.put(TITLE_NAME, new DataString(title));
        if (doc != null) result.put(DESCRIPTION_NAME, new DataString(doc));
        result.put(TYPE_NAME, new DataString("object"));
        final var requiredProperties = new DataList(DataString.DATATYPE);
        final var properties = new DataStruct();
        for (var field : structSchema.fields()) {
            properties.put(field.name(), fromDataSchema(field, definitions));
            if (field.required()) requiredProperties.add(new DataString(field.name()));
        }
        if (!requiredProperties.isEmpty()) result.put(REQUIRED_NAME, requiredProperties);
        if (properties.size() > 0) result.put(PROPERTIES_NAME, properties);
        result.put(ADDITIONAL_PROPERTIES, new DataBoolean(false));
        return result;
    }

    private DataStruct fromDataSchema(DataField field, DataStruct definitions) {
        final var result = new DataStruct();
        final var doc = field.doc();
        if (doc != null) result.put(DESCRIPTION_NAME, new DataString(doc));
        convertType(field.schema(), field.constant(), field.defaultValue(), result, definitions);
        return result;
    }

    private void convertType(DataSchema schema, boolean constant, DataValue defaultValue, DataStruct target, DataStruct definitions) {
        if (schema.type() == DataSchema.Type.NULL) target.put(TYPE_NAME, new DataString("null"));
        if (schema.type() == DataSchema.Type.BOOLEAN) target.put(TYPE_NAME, new DataString("boolean"));
        if (schema.type() == DataSchema.Type.LONG) target.put(TYPE_NAME, new DataString("number"));
        if (schema.type() == DataSchema.Type.STRING) {
            if (constant && defaultValue != null && defaultValue.value() != null) {
                DataList enumList = new DataList();
                enumList.add(new DataString(defaultValue.value().toString()));
                target.put(ENUM_NAME, enumList);
            } else {
                target.put(TYPE_NAME, new DataString("string"));
            }
        }
        if (schema instanceof ListSchema listSchema) {
            target.put(TYPE_NAME, new DataString("array"));
            final var subStruct = new DataStruct();
            convertType(listSchema.valueSchema(), false, null, subStruct, definitions);
            target.put(ITEMS_NAME, subStruct);
        }
        if (schema instanceof MapSchema mapSchema) {
            final var patternSubStruct = new DataStruct();
            target.put(PATTERN_PROPERTIES_NAME, patternSubStruct);


            final var subStruct = new DataStruct();
            target.put(TYPE_NAME, new DataString("object"));
            patternSubStruct.put(ALL_PROPERTIES_REGEX, subStruct);
            convertType(mapSchema.valueSchema(), false, null, subStruct, definitions);
        }
        if (schema instanceof StructSchema structSchema) {
            final var name = structSchema.name();
            if (!definitions.containsKey(name)) {
                definitions.put(name, fromDataSchema(structSchema, definitions));
            }
            target.put(REF_NAME, new DataString("#/definitions/" + name));
        }

        if (schema instanceof UnionSchema unionSchema) {
            // Convert to an array of possible types
            final var possibleTypes = new DataList();
            for (var possibleSchema : unionSchema.possibleSchemas()) {
                final var typeStruct = new DataStruct();
                convertType(possibleSchema, false, null, typeStruct, definitions);
                possibleTypes.add(typeStruct);
            }
            target.put(ANY_OF_NAME, possibleTypes);
        }
    }
}
