package io.axual.ksml.data.notation.xml;

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
import io.axual.ksml.data.notation.ReferenceResolver;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.schema.DataField;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.DataValue;
import io.axual.ksml.data.schema.StructSchema;

import java.util.ArrayList;
import java.util.List;

// This class maps an XSD schema to the internal DataSchema format. It does this
// in two steps, where the first step reads in the schema as plain XML. The second
// step parses the DataObject structure and translates the schema contents to the
// internal format. The fromDataSchema method does the reverse.
//
// Note: this is a rough implementation of parsing an XSD. Not all field types
// and complex data structures are supported yet. Feel free to complement.
//
// Improvement suggestions:
// - namespace handling
// - completing primitive and structured types
// - code polishing and documenting

public class XmlSchemaMapper implements DataSchemaMapper<String> {
    private static final XmlDataObjectMapper OBJECT_MAPPER = new XmlDataObjectMapper(true);
    private static final String ELEMENT_NAME = "element";
    private static final String NAME_NAME = "name";
    private static final String TYPE_NAME = "type";
    private static final String COMPLEX_TYPE_NAME = "complexType";
    private static final String SEQUENCE_NAME = "sequence";
    private static final String ANY_NAME = "any";
    private static final String BOOLEAN_NAME = "boolean";
    private static final String DECIMAL_NAME = "decimal";
    private static final String INTEGER_NAME = "integer";
    private static final String LONG_NAME = "long";
    private static final String STRING_NAME = "string";
    private static final String NAMESPACE_NAME = "xs";
    private static final String NAMESPACE_PREFIX = NAMESPACE_NAME + ":";
    private static final String TARGET_NAMESPACE_NAME = "targetNamespace";
    private static final String DEFAULT_NAME = "default";
    private static final String FIXED_NAME = "fixed";
    private static final String USE_NAME = "use";

    private record XsdParseContext(String namespace, String name, DataStruct context,
                                   ReferenceResolver<DataStruct> referenceResolver) {
    }

    @Override
    public DataSchema toDataSchema(String namespace, String name, String schemaString) {
        final var xsd = OBJECT_MAPPER.toDataObject(schemaString);
        if (!(xsd instanceof DataStruct schemaStruct)) {
            throw new SchemaException("Can not parse XML Schema: " + name);
        }

        final var targetNamespace = schemaStruct.getAsString(TARGET_NAMESPACE_NAME);
        final ReferenceResolver<DataStruct> referenceResolver = referenceName -> {
            final var result = schemaStruct.get(referenceName);
            if (result instanceof DataStruct resultStruct) return resultStruct;
            return null;
        };
        final var context = new XsdParseContext(targetNamespace != null ? targetNamespace.value() : namespace, name, schemaStruct, referenceResolver);
        for (final var item : schemaStruct.entrySet()) {
            final var result = parseType(context, item.getKey(), item.getValue());
            if (result != null && result.name().equals(name)) return result;
        }
        return null;
    }

    private StructSchema parseType(XsdParseContext context, String name, DataObject value) {
        if (name.equals(ELEMENT_NAME) && value instanceof DataStruct elementStruct) {
            return parseElement(context, elementStruct);
        }
        if (name.equals(COMPLEX_TYPE_NAME) && value instanceof DataStruct elementStruct) {
            return parseComplexType(context, null, elementStruct);
        }
        return null;
    }

    private StructSchema parseElement(XsdParseContext context, DataStruct elementStruct) {
        final var complexTypeChild = elementStruct.get(COMPLEX_TYPE_NAME);

        if (!(complexTypeChild instanceof DataStruct complexTypeStruct))
            throw schemaError(context, "Can not parse element");

        final var elementName = elementStruct.get(NAME_NAME);
        return parseComplexType(context, elementName instanceof DataString elementStr ? elementStr.value() : null, complexTypeStruct);
    }

    private StructSchema parseComplexType(XsdParseContext context, String name, DataStruct complexTypeStruct) {
        if (name == null) {
            final var complexTypeName = complexTypeStruct.get(NAME_NAME);
            if (complexTypeName instanceof DataString complexTypeNameStr)
                name = complexTypeNameStr.value();
        }

        final var sequence = complexTypeStruct.get(SEQUENCE_NAME);
        if (sequence instanceof DataStruct sequenceStruct) {
            final var element = sequenceStruct.get(ELEMENT_NAME);
            if (element instanceof DataStruct fieldStruct)
                return new StructSchema(context.namespace(), name, "Converted from XSD", List.of(parseField(context, fieldStruct)));
            if (element instanceof DataList fieldList)
                return new StructSchema(context.namespace(), name, "Converted from XSD", parseFields(context, fieldList));
        }

        throw schemaError(context, "Can not parse complexType");
    }

    private List<DataField> parseFields(XsdParseContext context, DataList fields) {
        final var result = new ArrayList<DataField>();
        for (final var field : fields) {
            if (!(field instanceof DataStruct fieldStruct))
                throw schemaError(context, "Field incorrectly specified");
            result.add(parseField(context, fieldStruct));
        }
        return result;
    }

    private DataField parseField(XsdParseContext context, DataStruct fieldStruct) {
        final var fieldName = fieldStruct.get(NAME_NAME);
        if (!(fieldName instanceof DataString fieldNameStr) || fieldNameStr.isEmpty())
            throw schemaError(context, "Field name missing in complexType element");

        // Parse inline complex type
        final var complexType = fieldStruct.get(COMPLEX_TYPE_NAME);
        if (complexType instanceof DataStruct complexTypeStruct) {
            final var schema = parseComplexType(context, null, complexTypeStruct);
            return new DataField(fieldNameStr.value(), schema);
        }

        // If not a complex type, then expect a type attribute
        final var fieldType = fieldStruct.get(TYPE_NAME);
        final String type;
        if (!(fieldType instanceof DataString fieldTypeStr) || fieldTypeStr.isEmpty()) {
            type = ANY_NAME;
        } else {
            type = fieldTypeStr.value().startsWith(NAMESPACE_PREFIX) ? fieldTypeStr.value().substring(NAMESPACE_PREFIX.length()) : fieldTypeStr.value();
        }
        final var fixedValue = fieldStruct.getAsString(FIXED_NAME);
        final var defaultValue = fixedValue != null ? fixedValue : fieldStruct.getAsString(DEFAULT_NAME);
        final var required = fieldStruct.getAsString(USE_NAME);
        return createDataFieldFrom(
                context,
                fieldNameStr.value(),
                type,
                required != null && "required".equals(required.value()),
                fixedValue != null,
                defaultValue != null ? defaultValue.value() : null);
    }

    private DataField createDataFieldFrom(XsdParseContext context, String name, String type, boolean required, boolean constant, String defaultValue) throws SchemaException {
        var schema = switch (type) {
            case ANY_NAME -> DataSchema.ANY_SCHEMA;
            case BOOLEAN_NAME -> DataSchema.BOOLEAN_SCHEMA;
            case DECIMAL_NAME -> DataSchema.DOUBLE_SCHEMA;
            case INTEGER_NAME -> DataSchema.INTEGER_SCHEMA;
            case LONG_NAME -> DataSchema.LONG_SCHEMA;
            case STRING_NAME -> DataSchema.STRING_SCHEMA;
            default -> null;
        };

        if (schema == null) {
            final var refType = context.referenceResolver().get(type);
            if (refType != null) schema = parseType(context, type, refType);
        }

        final var dv = defaultValue != null ? new DataValue(defaultValue) : null;
        return new DataField(name, schema, null, DataField.NO_TAG, required, constant, dv);
    }

    private SchemaException schemaError(XsdParseContext context, String message) {
        return new SchemaException("Can not parse XML Schema " + context.namespace() + "/" + context.name() + ": " + message);
    }

    @Override
    public String fromDataSchema(DataSchema schema) {
        return null;
    }
}
