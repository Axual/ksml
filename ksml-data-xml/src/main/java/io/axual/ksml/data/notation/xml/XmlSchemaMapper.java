package io.axual.ksml.data.notation.xml;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - XML
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
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
import io.axual.ksml.data.schema.*;
import io.axual.ksml.data.type.Symbol;
import org.apache.ws.commons.schema.*;
import org.apache.ws.commons.schema.utils.NamespaceMap;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.stream.StreamSource;
import java.io.ByteArrayInputStream;
import java.io.StringWriter;
import java.util.*;
import java.util.function.Supplier;

import static org.apache.ws.commons.schema.constants.Constants.*;

public class XmlSchemaMapper implements DataSchemaMapper<String> {
    private static final String NAMESPACE_NAME = "xs";

    @Override
    public DataSchema toDataSchema(String namespace, String name, String schemaString) {
        final var is = new ByteArrayInputStream(schemaString.getBytes());
        final var schemaCol = new XmlSchemaCollection();
        final var schema = schemaCol.read(new StreamSource(is));
        final var element = schema.getElementByName(name);
        if (element == null) throw new SchemaException("Schema element not found: " + name);
        return convertElementToStruct(new XMLSchemaParseContext(namespace, schema), element);
    }

    private static class XMLSchemaParseContext {
        private final String namespace;
        private final XmlSchema schema;

        public XMLSchemaParseContext(String namespace, XmlSchema schema) {
            this.namespace = namespace;
            this.schema = schema;
        }

        public XmlSchemaType findType(QName name) {
            for (final var entry : schema.getSchemaTypes().entrySet()) {
                if (entry.getKey().getLocalPart().equals(name.getLocalPart()))
                    return entry.getValue();
            }
            throw new SchemaException("Schema type not found: " + name);
        }
    }

    private StructSchema convertElementToStruct(XMLSchemaParseContext context, XmlSchemaElement element) {
        if (element.getSchemaType() instanceof XmlSchemaComplexType complexType && complexType.getParticle() instanceof XmlSchemaSequence sequence) {
            final var fields = convertToFields(context, sequence);
            return new StructSchema(context.namespace, element.getName(), extractDoc(element.getAnnotation()), fields, false);
        }
        return null;
    }

    private String extractDoc(XmlSchemaAnnotation annotation) {
        if (annotation == null) return null;
        final var result = new StringBuilder();
        for (final var item : annotation.getItems()) {
            if (item instanceof XmlSchemaDocumentation doc) {
                for (var index = 0; index < doc.getMarkup().getLength(); index++) {
                    if (!result.isEmpty()) result.append('\n');
                    result.append(doc.getMarkup().item(index).getTextContent());
                }
            }
        }
        return result.toString();
    }

    private List<DataField> convertToFields(XMLSchemaParseContext context, XmlSchemaSequence sequence) {
        final var fields = new ArrayList<DataField>();
        for (final var field : sequence.getItems()) {
            fields.add(convertSequenceMemberToDataField(context, field));
        }
        return fields;
    }

    private DataField convertSequenceMemberToDataField(XMLSchemaParseContext context, XmlSchemaSequenceMember member) {
        if (member instanceof XmlSchemaElement element) {
            DataSchema schema = null;
            if (element.getSchemaTypeName() != null) {
                schema = convertToSchema(element.getSchemaTypeName());
            }
            if (schema == null) {
                final var type = element.getSchemaType() != null ? element.getSchemaType() : context.findType(element.getSchemaTypeName());
                schema = convertToSchema(context, type);
            }
            final var doc = extractDoc(element.getAnnotation());
            final var required = element.getMinOccurs() > 0;
            return new DataField(element.getName(), schema, doc, DataField.NO_TAG, required);
        }
        return null;
    }

    private DataSchema convertToSchema(XMLSchemaParseContext context, XmlSchemaType type) {
        if (type instanceof XmlSchemaSimpleType simpleType) {
            final var content = simpleType.getContent();
            if (content instanceof XmlSchemaSimpleTypeList list) {
                if (list.getItemTypeName() != null) {
                    final var itemType = convertToSchemaForced(list.getItemTypeName());
                    return new ListSchema(itemType);
                }
                final var itemType = convertToSchema(context, list.getItemType());
                return new ListSchema(itemType);
            }
            if (content instanceof XmlSchemaSimpleTypeRestriction restriction) {
                final var symbols = new ArrayList<Symbol>();
                for (final var facet : restriction.getFacets()) {
                    if (facet instanceof XmlSchemaEnumerationFacet enumFacet) {
                        final var value = enumFacet.getValue().toString();
                        symbols.add(new Symbol(value));
                    }
                }
                return new EnumSchema(null, type.getName(), extractDoc(type.getAnnotation()), symbols);
            }
            if (content instanceof XmlSchemaSimpleTypeUnion union) {
                final var memberTypes = new ArrayList<DataField>();
                for (final var member : union.getMemberTypesQNames()) {
                    final var schema = convertToSchemaForced(member);
                    memberTypes.add(new DataField(null, schema, null));
                }
                return new UnionSchema(memberTypes.toArray(DataField[]::new));
            }
        }
        if (type instanceof XmlSchemaComplexType complexType) {
            if (complexType.getParticle() instanceof XmlSchemaSequence sequence) {
                final var fields = convertToFields(context, sequence);
                return new StructSchema(null, complexType.getName(), extractDoc(complexType.getAnnotation()), fields, false);
            }
        }
        throw new SchemaException("Could not convert XSD type to DataSchema: " + type);
    }

    private DataSchema convertToSchemaForced(QName qname) {
        final var result = convertToSchema(qname);
        if (result != null) return result;
        throw new SchemaException("Could not convert QName to DataSchema: " + qname);
    }

    private DataSchema convertToSchema(QName qname) {
        if (XSD_ANY.getLocalPart().equals(qname.getLocalPart())) return DataSchema.ANY_SCHEMA;
        if (XSD_BOOLEAN.getLocalPart().equals(qname.getLocalPart())) return DataSchema.BOOLEAN_SCHEMA;
        if (XSD_BYTE.getLocalPart().equals(qname.getLocalPart())) return DataSchema.BYTE_SCHEMA;
        if (XSD_SHORT.getLocalPart().equals(qname.getLocalPart())) return DataSchema.SHORT_SCHEMA;
        if (XSD_INTEGER.getLocalPart().equals(qname.getLocalPart())) return DataSchema.INTEGER_SCHEMA;
        if (XSD_LONG.getLocalPart().equals(qname.getLocalPart())) return DataSchema.LONG_SCHEMA;
        if (XSD_DOUBLE.getLocalPart().equals(qname.getLocalPart())) return DataSchema.DOUBLE_SCHEMA;
        if (XSD_FLOAT.getLocalPart().equals(qname.getLocalPart())) return DataSchema.FLOAT_SCHEMA;
        if (XSD_BASE64.getLocalPart().equals(qname.getLocalPart())) return DataSchema.BYTES_SCHEMA;
        if (XSD_STRING.getLocalPart().equals(qname.getLocalPart())) return DataSchema.STRING_SCHEMA;
        return null;
    }

    @Override
    public String fromDataSchema(DataSchema schema) {
        if (schema instanceof StructSchema structSchema) {
            final var context = new XMLSchemaWriteContext(structSchema.namespace());
            convertToXml(context, structSchema);
            final var writer = new StringWriter();
            context.schema.write(writer);
            return writer.toString();
        }
        return null;
    }

    private static class XMLSchemaWriteContext {
        private final DocumentBuilder docBuilder;
        private final Document doc;
        private final XmlSchema schema;
        private final Map<String, XmlSchemaType> schemaTypes = new HashMap<>();

        public XMLSchemaWriteContext(String namespace) {
            try {
                docBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            } catch (ParserConfigurationException e) {
                throw new SchemaException("Could not create XML schema documentBuilder", e);
            }
            doc = docBuilder.newDocument();
            schema = new XmlSchema(namespace, new XmlSchemaCollection());
            final var map = new NamespaceMap();
            map.put(NAMESPACE_NAME, URI_2001_SCHEMA_XSD);
            schema.setNamespaceContext(map);
            schema.setAttributeFormDefault(XmlSchemaForm.NONE);
            schema.setElementFormDefault(XmlSchemaForm.NONE);
            schema.setSchemaNamespacePrefix("");
        }

        public XmlSchemaSimpleType simpleType(String name, String doc, XmlSchemaSimpleTypeContent content) {
            final var result = new XmlSchemaSimpleType(schema, name != null);
            if (name != null) {
                result.setName(name);
                schemaTypes.put(name, result);
            }
            XmlSchemaMapper.setDocAnnotation(this, result, doc);
            result.setContent(content);
            return result;
        }

        public XmlSchemaType type(String name, Supplier<XmlSchemaType> supplier) {
            if (schemaTypes.containsKey(name)) return schemaTypes.get(name);
            final var result = supplier.get();
            schemaTypes.put(name, result);
            return result;
        }
    }

    private XmlSchemaElement convertToXml(XMLSchemaWriteContext context, StructSchema schema) {
        final var result = new XmlSchemaElement(context.schema, true);
        result.setName(schema.name());
        setDocAnnotation(context, result, schema.doc());
        final var complexType = new XmlSchemaComplexType(context.schema, false);
        final var sequence = new XmlSchemaSequence();
        sequence.getItems().addAll(schema.fields().stream()
                .map(f -> convertToXml(context, f))
                .toList());
        complexType.setParticle(sequence);
        result.setType(complexType);
        return result;
    }

    private static void setDocAnnotation(XMLSchemaWriteContext context, XmlSchemaAnnotated annotated, String doc) {
        if (doc != null && !doc.isEmpty()) {
            final var annotation = new XmlSchemaAnnotation();
            final var xmlDoc = new XmlSchemaDocumentation();
            final var text = context.doc.createTextNode(doc);
            xmlDoc.setMarkup(new NodeList() {
                @Override
                public Node item(int index) {
                    return text;
                }

                @Override
                public int getLength() {
                    return 1;
                }
            });
            annotation.getItems().add(xmlDoc);
            annotated.setAnnotation(annotation);
        }
    }

    private XmlSchemaSequence convertToXml(XMLSchemaWriteContext context, List<DataField> fields) {
        final var result = new XmlSchemaSequence();
        result.getItems().addAll(fields.stream()
                .map(field -> convertToXml(context, field))
                .toList());
        return result;
    }

    private XmlSchemaElement convertToXml(XMLSchemaWriteContext context, DataField field) {
        final var result = new XmlSchemaElement(context.schema, false);
        result.setName(field.name());
        setDocAnnotation(context, result, field.doc());
        final var qname = convertToQName(field.schema());
        if (qname != null) {
            result.setSchemaTypeName(qname);
        } else {
            final var schema = convertSchema(context, field.schema());
            if (schema.getName() != null) {
                result.setSchemaTypeName(new QName(null, schema.getName(), NAMESPACE_NAME));
            } else {
                result.setSchemaType(schema);
            }
        }
        if (field.defaultValue() != null) {
            final var defaultValue = field.defaultValue().value() != null ? field.defaultValue().value().toString() : "null";
            result.setDefaultValue(defaultValue);
        }
        result.setMinOccurs(field.required() ? 1 : 0);
        result.setMaxOccurs(1);
        return result;
    }

    private XmlSchemaType convertSchema(XMLSchemaWriteContext context, DataSchema schema) {
        if (schema instanceof EnumSchema enumSchema) return convertToXml(context, enumSchema);
        if (schema instanceof ListSchema listSchema) return convertToXml(context, listSchema);
        if (schema instanceof StructSchema structSchema) return convertToComplexType(context, structSchema);
        if (schema instanceof UnionSchema unionSchema) return convertToXml(context, unionSchema);
        throw new SchemaException("Could not convert schema to XML schema: " + schema);
    }

    private XmlSchemaSimpleType convertToXml(XMLSchemaWriteContext context, EnumSchema schema) {
        final var restriction = new XmlSchemaSimpleTypeRestriction();
        restriction.setBaseTypeName(XSD_STRING);
        restriction.getFacets().addAll(
                schema.symbols().stream()
                        .map(s -> new XmlSchemaEnumerationFacet(s.name(), false))
                        .toList());
        return context.simpleType(schema.name(), schema.doc(), restriction);
    }

    private XmlSchemaSimpleType convertToXml(XMLSchemaWriteContext context, ListSchema schema) {
        final var itemType = convertToQName(schema.valueSchema());
        final var list = new XmlSchemaSimpleTypeList();
        list.setItemTypeName(itemType);
        return context.simpleType(null, null, list);
    }

    private XmlSchemaType convertToComplexType(XMLSchemaWriteContext context, StructSchema schema) {
        return context.type(schema.name(), () -> {
            final var result = new XmlSchemaComplexType(context.schema, true);
            result.setName(schema.name());
            result.setParticle(convertToXml(context, schema.fields()));
            return result;
        });
    }

    private XmlSchemaSimpleType convertToXml(XMLSchemaWriteContext context, UnionSchema schema) {
        final var union = new XmlSchemaSimpleTypeUnion();
        union.setMemberTypesQNames(Arrays.stream(schema.memberSchemas())
                .map(m -> convertToQName(m.schema()))
                .toArray(QName[]::new));
        return context.simpleType(null, null, union);
    }

    private QName convertToQName(DataSchema schema) {
        if (schema == DataSchema.ANY_SCHEMA) return XSD_ANY;
        if (schema == DataSchema.BOOLEAN_SCHEMA) return XSD_BOOLEAN;
        if (schema == DataSchema.BYTE_SCHEMA) return XSD_BYTE;
        if (schema == DataSchema.SHORT_SCHEMA) return XSD_SHORT;
        if (schema == DataSchema.INTEGER_SCHEMA) return XSD_INTEGER;
        if (schema == DataSchema.LONG_SCHEMA) return XSD_LONG;
        if (schema == DataSchema.DOUBLE_SCHEMA) return XSD_DOUBLE;
        if (schema == DataSchema.FLOAT_SCHEMA) return XSD_FLOAT;
        if (schema == DataSchema.BYTES_SCHEMA) return XSD_BASE64;
        if (schema == DataSchema.STRING_SCHEMA) return XSD_STRING;
        return null;
    }
}
