package io.axual.ksml.notation.xml;

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
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.schema.DataField;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.execution.FatalError;

import java.util.ArrayList;
import java.util.List;

import static io.axual.ksml.notation.xml.XmlDataObjectMapper.ATTRIBUTES_ELEMENT_NAME;
import static io.axual.ksml.notation.xml.XmlDataObjectMapper.COUNT_SYMBOL;

// This class maps an XSD schema to the internal DataSchema format. It does this
// in two steps, where the first step reads in the schema as plain XML. The second
// step parses the DataObject structure and translates the schema contents to the
// internal format. The fromDataSchema method does the reverse.
//
// Note: this is a first rough implementation of parsing an XSD. Not all field types
// and complex data structures are supported yet. Feel free to complement.
//
// Improvement suggestions:
// - namespace handling
// - completing primitive and structured types
// - code polishing and documenting
//
// An example schema:
// <?xml version="1.0" encoding="UTF-8"?>
// <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
//     <xs:element name="note">
//         <xs:complexType>
//             <xs:sequence>
//                 <xs:element name="to" type="xs:string"/>
//                 <xs:element name="from" type="xs:string"/>
//                 <xs:element name="heading" type="xs:string"/>
//                 <xs:element name="body" type="xs:string"/>
//             </xs:sequence>
//         </xs:complexType>
//     </xs:element>
// </xs:schema>
//
// Translates into the following intermediate structure during translation:
// {
//  ".attributes": {
//    "xmlns:xs": "http://www.w3.org/2001/XMLSchema"
//  },
//  "element#1": {
//    ".attributes": {
//      "name": "note"
//    },
//    "complexType": {
//      "sequence": {
//        "element#1": {
//          ".attributes": {
//            "name": "to",
//            "type": "xs:string"
//          }
//        },
//        "element#2": {
//          ".attributes": {
//            "name": "from",
//            "type": "xs:string"
//          }
//        },
//        "element#3": {
//          ".attributes": {
//            "name": "heading",
//            "type": "xs:string"
//          }
//        },
//        "element#4": {
//          ".attributes": {
//            "name": "body",
//            "type": "xs:string"
//          }
//        }
//      }
//    }
//  }
//}
public class XmlSchemaMapper implements DataSchemaMapper<String> {
    private static final XmlDataObjectMapper MAPPER = new XmlDataObjectMapper();
    private static final String ELEMENT_NAME = "element";
    private static final String NAME_NAME = "name";
    private static final String TYPE_NAME = "type";
    private static final String COMPLEX_TYPE_NAME = "complexType";
    private static final String SEQUENCE_NAME = "sequence";

    @Override
    public StructSchema toDataSchema(String namespace, String name, String schema) {
        var parsedSchema = MAPPER.toDataObject(schema);
        if (parsedSchema instanceof DataStruct schemaStruct) {
            var containedSchema = findSchema(schemaStruct, name);
            if (containedSchema instanceof DataStruct containedSchemaStruct) {
                var field = parseField(containedSchemaStruct);
                if (field.schema() instanceof StructSchema fieldStructSchema) return fieldStructSchema;
            }
        }
        throw FatalError.schemaError("Can not parse XML Schema: " + name);
    }

    private DataObject findSchema(DataStruct container, String name) {
        var firstAttempt = findSchemaByChild(container.get(ELEMENT_NAME), name);
        if (firstAttempt != null) return firstAttempt;
        var index = 1;
        while (container.containsKey(ELEMENT_NAME + COUNT_SYMBOL + index)) {
            var attempt = findSchemaByChild(container.get(ELEMENT_NAME + COUNT_SYMBOL + index), name);
            if (attempt != null) return attempt;
        }
        return null;
    }

    private DataObject findSchemaByChild(DataObject child, String name) {
        if (child instanceof DataStruct childStruct) {
            var attributes = childStruct.get(ATTRIBUTES_ELEMENT_NAME);
            if (attributes instanceof DataStruct attrributeStruct) {
                var childName = attrributeStruct.get(NAME_NAME);
                if (childName instanceof DataString childNameStr && childNameStr.value().equals(name)) return child;
            }
        }
        return null;
    }

    private List<DataField> parseFields(DataStruct object) {
        var result = new ArrayList<DataField>();
        var fieldChild = object.get(ELEMENT_NAME);
        if (fieldChild instanceof DataStruct fieldStruct) {
            var field = parseField(fieldStruct);
            if (field != null) result.add(field);
        }
        var index = 1;
        while (object.containsKey(ELEMENT_NAME + COUNT_SYMBOL + index)) {
            fieldChild = object.get(ELEMENT_NAME + COUNT_SYMBOL + index);
            if (fieldChild instanceof DataStruct fieldStruct) {
                var field = parseField(fieldStruct);
                if (field != null) result.add(field);
            }
            index++;
        }
        return result;
    }

    private DataField parseField(DataStruct fieldStruct) {
        var fieldName = fieldStruct.get(ATTRIBUTES_ELEMENT_NAME) instanceof DataStruct attributeStruct ? attributeStruct.get(NAME_NAME) : null;
        if (fieldName != null) {
            var fieldType = fieldStruct.get(ATTRIBUTES_ELEMENT_NAME) instanceof DataStruct attributeStruct ? attributeStruct.get(TYPE_NAME) : null;
            if (fieldType instanceof DataString fieldTypeString) {
                var type = fieldTypeString.value().contains(":") ? fieldTypeString.value().substring(fieldTypeString.value().indexOf(":") + 1) : fieldTypeString.value();
                return simpleField(fieldName.toString(), type);
            } else {
                // Field type is not specified, so dig down into the elements below to find out the type
                var complexTypeElement = fieldStruct.get(COMPLEX_TYPE_NAME);
                if (complexTypeElement instanceof DataStruct complexTypeStruct) {
                    var sequenceElement = complexTypeStruct.get(SEQUENCE_NAME);
                    if (sequenceElement instanceof DataStruct sequenceStruct) {
                        var fields = parseFields(sequenceStruct);
                        return new DataField(fieldName.toString(), new StructSchema(null, fieldName.toString(), "Converted from XSD", fields), null);
                    }
                }
            }
        }
        return null;
    }

    private DataField simpleField(String name, String type) {
        var schema = switch (type) {
            case "any" -> DataSchema.create(DataSchema.Type.ANY);
            case "boolean" -> DataSchema.create(DataSchema.Type.BOOLEAN);
            case "integer" -> DataSchema.create(DataSchema.Type.INTEGER);
            case "long" -> DataSchema.create(DataSchema.Type.LONG);
            case "string" -> DataSchema.create(DataSchema.Type.STRING);
            default -> null;
        };
        if (schema == null) return null;
        return new DataField(name, schema, null);
    }

    @Override
    public String fromDataSchema(DataSchema schema) {
        if (schema instanceof StructSchema structSchema) {
            var codedSchema = encodeSchema(structSchema);
            return MAPPER.fromDataObject(codedSchema);
        }
        return null;
    }

    private DataStruct encodeSchema(StructSchema schema) {
        var prefix = "xs:";
        var result = new DataStruct(new StructSchema(null, prefix + schema.name(), null, null));
        var attributes = new DataStruct();
        attributes.put("xmlns:xs", new DataString("http://www.w3.org/2001/XMLSchema"));
        result.put(ATTRIBUTES_ELEMENT_NAME, attributes);
        var elementCount = 0;
        for (var field : schema.fields()) {
            DataStruct fieldStruct = new DataStruct();
            result.put(prefix + ELEMENT_NAME + COUNT_SYMBOL + (++elementCount), fieldStruct);
            attributes = new DataStruct();
            fieldStruct.put(ATTRIBUTES_ELEMENT_NAME, attributes);
            attributes.put(NAME_NAME, new DataString(field.name()));
            var fieldSchema = field.schema();
            var fieldType = simpleType(fieldSchema.type());
            if (fieldType != null) {
                attributes.put(TYPE_NAME, new DataString(prefix + fieldType));
            } else {
                if (fieldSchema instanceof StructSchema fieldStructSchema) {
                    var complexTypeStruct = new DataStruct();
                    fieldStruct.put(prefix + COMPLEX_TYPE_NAME, complexTypeStruct);
                    var sequenceStruct = encodeSchema(fieldStructSchema);
                    complexTypeStruct.put(prefix + SEQUENCE_NAME, sequenceStruct);
                }
            }
        }

        return result;
    }

    public String simpleType(DataSchema.Type type) {
        return switch (type) {
            case ANY -> "any";
            case BOOLEAN -> "boolean";
            case INTEGER -> "integer";
            case LONG -> "long";
            case STRING -> "string";
            default -> null;
        };
    }
}
