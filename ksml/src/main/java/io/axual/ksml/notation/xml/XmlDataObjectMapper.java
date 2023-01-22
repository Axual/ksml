package io.axual.ksml.notation.xml;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 Axual B.V.
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

import io.axual.ksml.data.mapper.DataObjectMapper;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.execution.FatalError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.ByteArrayInputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;

public class XmlDataObjectMapper implements DataObjectMapper<String> {
    private static final Logger LOG = LoggerFactory.getLogger(XmlDataObjectMapper.class);
    private static final String COUNT_SYMBOL = "#";
    public static final String ATTRIBUTES_ELEMENT_NAME = ".attributes";

    private final DocumentBuilder documentBuilder;
    private final Transformer transformer;

    private interface ElementCreator {
        Element create(String name);
    }

    public XmlDataObjectMapper() {
        try {
            // Set up the document builder for creating future documents
            var documentBuilderFactory = DocumentBuilderFactory.newInstance();
            documentBuilder = documentBuilderFactory.newDocumentBuilder();

            // Set up the transformer for later conversion from DOM to XML
            TransformerFactory tf = TransformerFactory.newInstance();
            transformer = tf.newTransformer();
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
            transformer.setOutputProperty(OutputKeys.INDENT, "no");
        } catch (Exception e) {
            throw FatalError.executionError("Could not initialize XML Document Builder", e);
        }
    }

    @Override
    public DataObject toDataObject(DataType expected, String value) {
        try {
            var doc = documentBuilder.parse(new ByteArrayInputStream(value.getBytes()));
            doc.getDocumentElement().normalize();
            return elementToDataObject(doc.getDocumentElement());
        } catch (Exception e) {
            throw FatalError.dataError("Could not parse XML", e);
        }
    }

    private DataObject valueToDataObject(Node node) {
        var nodeType = node.getNodeType();
        if (nodeType == Node.ELEMENT_NODE) {
            var childNode = node.getFirstChild();
            if (childNode != null && childNode.getNextSibling() == null && childNode.getNodeType() == Node.TEXT_NODE) {
                return stringToDataObject(childNode.getTextContent());
            }
            return elementToDataObject((Element) node);
        }
        if (nodeType == Node.TEXT_NODE) {
            return stringToDataObject(node.getTextContent());
        }
        LOG.warn("Unknown node type in XML body: {}", node.getNodeType());
        return null;
    }

    private DataStruct elementToDataObject(Element element) {
        var elementName = element.getNodeName();
        var result = new DataStruct(new StructSchema(null, elementName, "ConvertedFromXML", null));

        // Store all attributes in the result as dot-prefixed names
        var attributes = element.getAttributes();
        if (attributes != null && attributes.getLength() > 0) {
            var attributeStruct = new DataStruct();
            result.put(ATTRIBUTES_ELEMENT_NAME, attributeStruct);
            for (int index = 0; index < attributes.getLength(); index++) {
                var attribute = attributes.item(index);
                var name = attribute.getNodeName();
                var value = attribute.getNodeValue();
                attributeStruct.put(name, DataString.from(value));
            }
        }

        // Analyze and convert the child elements
        var childNames = new ArrayList<String>();
        var childValues = new ArrayList<DataObject>();
        var childNameCount = new HashMap<String, Long>();

        Node child = element.getFirstChild();
        while (child != null) {
            var name = child.getNodeName();
            var value = valueToDataObject(child);
            if (value != null) {
                // Add the name and value to the lists
                childNames.add(name);
                childValues.add(value);

                // Increase the name counter
                var count = childNameCount.get(name);
                if (count == null) count = 0L;
                childNameCount.put(name, count + 1);
            }
            child = child.getNextSibling();
        }

        // At this point the lists contain the in-order children of the element. We traverse the list backwards to
        // properly encode the order of child elements using the COUNT_SYMBOL.
        var childNameNumbering = new HashMap<String, Long>();
        for (int index = 0; index < childNames.size(); index++) {
            var name = childNames.get(index);
            var value = childValues.get(index);
            var nameCount = childNameCount.get(name);
            if (nameCount != null && nameCount > 1) {
                var childNameNumber = childNameNumbering.get(name);
                if (childNameNumber == null) childNameNumber = 0L;
                childNameNumbering.put(name, childNameNumber + 1);
                name = name + COUNT_SYMBOL + childNameNumber;
            }
            result.put(name, value);
        }

        return result;
    }

    private DataString stringToDataObject(String content) {
        if (content == null) return null;
        content = content.replaceAll("\n", "").trim();
        if (!content.isEmpty()) return new DataString(content);
        return null;
    }

    @Override
    public String fromDataObject(DataObject value) {
        var doc = documentBuilder.newDocument();
        if (value instanceof DataStruct valueStruct) {
            var rootName = valueStruct.type().schemaName();
            var rootElement = doc.createElement(rootName);
            elementFromDataObject(doc::createElement, rootElement, valueStruct);
            doc.appendChild(rootElement);
            doc.setXmlStandalone(true);

            try {
                var writer = new StringWriter();
                transformer.transform(new DOMSource(doc), new StreamResult(writer));
                return writer.toString();
            } catch (TransformerException e) {
                throw FatalError.executionError("Could not transform value to XML", e);
            }
        }
        throw FatalError.executionError("Could not transform value to XML");
    }

    private void elementFromDataObject(ElementCreator elementCreator, Element element, DataObject value) {
        if (value instanceof DataStruct valueStruct) {
            // Convert struct attributes to DOM element attributes
            var as = valueStruct.get(ATTRIBUTES_ELEMENT_NAME);
            if (as instanceof DataStruct attributeStruct) {
                for (var attribute : attributeStruct.entrySet()) {
                    var attributeName = attribute.getKey();
                    var attributeValue = attribute.getValue().toString();
                    element.setAttribute(attributeName, attributeValue);
                }
            }

            // Convert struct elements to DOM elements
            for (var childObject : valueStruct.entrySet()) {
                var elementName = childObject.getKey();
                if (!elementName.equals(ATTRIBUTES_ELEMENT_NAME)) {
                    if (elementName.contains(COUNT_SYMBOL))
                        elementName = elementName.substring(0, elementName.indexOf(COUNT_SYMBOL));
                    var elementValue = childObject.getValue();
                    var childElement = elementCreator.create(elementName);
                    element.appendChild(childElement);
                    elementFromDataObject(elementCreator, childElement, elementValue);
                }
            }
            return;
        }
        if (value != null) {
            element.setTextContent(value.toString());
        }
    }
}
