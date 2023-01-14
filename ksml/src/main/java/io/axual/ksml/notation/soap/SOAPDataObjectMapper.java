package io.axual.ksml.notation.soap;

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

import io.axual.ksml.data.mapper.DataObjectMapper;
import io.axual.ksml.data.object.*;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.exception.KSMLDataException;
import io.axual.ksml.execution.FatalError;
import io.axual.ksml.notation.xml.XmlDataObjectMapper;
import io.axual.ksml.schema.AnySchema;
import jakarta.xml.soap.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;

import javax.xml.namespace.QName;

import static io.axual.ksml.dsl.SOAPSchema.*;

public class SOAPDataObjectMapper implements DataObjectMapper<SOAPMessage> {
    private static final Logger LOG = LoggerFactory.getLogger(SOAPDataObjectMapper.class);

    private final MessageFactory messageFactory;
    private final XmlDataObjectMapper xmlMapper = new XmlDataObjectMapper();

    public SOAPDataObjectMapper() {
        try {
            messageFactory = MessageFactory.newInstance(SOAPConstants.SOAP_1_2_PROTOCOL);
        } catch (SOAPException e) {
            throw FatalError.executionError("Could not create SOAP Message Factory", e);
        }
    }

    @Override
    public DataObject toDataObject(DataType expected, SOAPMessage value) {
        return convertMessage(value);
    }

    private DataObject convertMessage(SOAPMessage message) {
        var result = new DataStruct(generateSOAPSchema(AnySchema.INSTANCE));

        try {
            // Convert the envelope
            result.put(SOAP_SCHEMA_ENVELOPE_FIELD, convertEnvelope(message.getSOAPPart().getEnvelope()));
        } catch (SOAPException e) {
            throw FatalError.reportAndExit(new KSMLDataException("Could not convert SOAP Part to DataObject", e));
        }

        return result;
    }

    private DataObject convertEnvelope(SOAPEnvelope envelope) throws SOAPException {
        var result = new DataStruct(generateEnvelopeSchema(AnySchema.INSTANCE));

        // Convert the SOAP header
        if (envelope.getHeader() != null) {
            result.put(SOAP_SCHEMA_HEADER_FIELD, convertHeader(envelope.getHeader()));
        }

        // Convert body
        if (envelope.getBody() != null) {
            var body = new DataStruct(generateBodySchema(AnySchema.INSTANCE));
            result.put(SOAP_SCHEMA_BODY_FIELD, body);
            body.putIfNotNull(SOAP_SCHEMA_ENCODING_STYLE_FIELD, DataString.from(envelope.getBody().getEncodingStyle()));

            // Convert body elements
            var bodyElements = new DataList();
            body.put(SOAP_SCHEMA_BODY_ELEMENTS_FIELD, bodyElements);
            envelope.getBody().getChildElements().forEachRemaining(element -> bodyElements.addIfNotNull(convertBodyElement(element)));
        }

        // Convert fault
        if (envelope.getBody().hasFault())
            result.put(SOAP_SCHEMA_FAULT_FIELD, convertFault(envelope.getBody().getFault()));

        return result;
    }

    private DataObject convertBodyElement(Node element) {
        switch (element.getNodeType()) {
            case Node.ELEMENT_NODE -> {
                return convertListElement(element);
            }
            case Node.TEXT_NODE -> {
                return convertStringElement(element.getTextContent());
            }
            default -> {
                LOG.warn("Unknown node type in SOAP body: {}", element.getNodeType());
                return null;
            }
        }
    }

    private DataString convertStringElement(String content) {
        if (content == null) return null;
        content = content.replaceAll("\n", "").trim();
        if (!content.isEmpty()) return new DataString(content);
        return null;
    }

    private DataStruct convertListElement(Node element) {
        DataStruct result = new DataStruct();
        DataList children = new DataList();
        Node child = element.getFirstChild();
        while (child != null) {
            var value = convertBodyElement(child);
            if (value != null) children.add(value);
            child = child.getNextSibling();
        }
        if (children.size() == 1) {
            result.put(element.getNodeName(), children.get(0));
        } else {
            result.put(element.getNodeName(), children);
        }
        return result;
    }

    private DataObject convertFault(SOAPFault fault) {
        var result = new DataStruct(SOAP_FAULT_SCHEMA);
        result.put(SOAP_SCHEMA_QNAME_FIELD, convertQName(fault.getElementQName()));
        result.putIfNotNull(SOAP_SCHEMA_FAULT_ACTOR_FIELD, DataString.from(fault.getFaultActor()));
        result.putIfNotNull(SOAP_SCHEMA_FAULT_CODE_FIELD, DataString.from(fault.getFaultCode()));
        result.putIfNotNull(SOAP_SCHEMA_FAULT_STRING_FIELD, DataString.from(fault.getFaultString()));
        result.putIfNotNull(SOAP_SCHEMA_FAULT_DETAIL_FIELD, convertFaultDetail(fault.getDetail()));
        return result;
    }

    private DataObject convertFaultDetail(Detail detail) {
        if (detail == null) return null;

        var detailEntries = new DataList(new StructType(SOAP_FAULT_DETAIL_SCHEMA));
        detail.getDetailEntries().forEachRemaining(entry -> {
            var detailEntry = new DataStruct(SOAP_FAULT_DETAIL_SCHEMA);
            var content = DataString.from(entry.getValue());
            detailEntry.put(SOAP_SCHEMA_QNAME_FIELD, convertQName(entry.getElementQName()));
            detailEntry.put(SOAP_SCHEMA_FAULT_DETAIL_FIELD, content);
            detailEntries.add(detailEntry);
        });

        return !detailEntries.isEmpty() ? detailEntries : null;
    }

    private DataObject convertHeader(SOAPHeader header) throws SOAPException {
        if (header == null) return null;

        // Convert all header elements
        var result = new DataStruct(SOAP_HEADER_SCHEMA);

        var headerElements = new DataList(new StructType(SOAP_HEADER_ELEMENT_SCHEMA));
        result.put(SOAP_SCHEMA_HEADER_ELEMENTS_FIELD, headerElements);
        header.extractAllHeaderElements().forEachRemaining(element -> headerElements.addIfNotNull(convertHeaderElement(element)));

        return result;
    }

    private DataObject convertHeaderElement(SOAPHeaderElement element) {
        if (element == null) return null;

        var result = new DataStruct(SOAP_HEADER_ELEMENT_SCHEMA);
        result.put(SOAP_SCHEMA_QNAME_FIELD, convertQName(element.getElementQName()));
        result.putIfNotNull(SOAP_SCHEMA_ACTOR_FIELD, DataString.from(element.getActor()));
        result.putIfNotNull(SOAP_SCHEMA_ROLE_FIELD, DataString.from(element.getRole()));
        result.put(SOAP_SCHEMA_MUST_UNDERSTAND_FIELD, new DataBoolean(element.getMustUnderstand()));
        result.put(SOAP_SCHEMA_RELAY_FIELD, new DataBoolean(element.getRelay()));

        return result;
    }

    private DataObject convertQName(QName qname) {
        if (qname == null) return null;
        var result = new DataStruct(SOAP_QNAME_SCHEMA);
        result.putIfNotNull(SOAP_SCHEMA_QNAME_LOCAL_PART_FIELD, DataString.from(qname.getLocalPart()));
        result.putIfNotNull(SOAP_SCHEMA_QNAME_NAMESPACE_URI_FIELD, DataString.from(qname.getNamespaceURI()));
        result.putIfNotNull(SOAP_SCHEMA_QNAME_PREFIX_FIELD, DataString.from(qname.getPrefix()));
        return result;
    }

    @Override
    public SOAPMessage fromDataObject(DataObject value) {
        try {
            var result = messageFactory.createMessage();
            if (value instanceof DataStruct message) {
                if (message.get(SOAP_SCHEMA_ENVELOPE_FIELD) instanceof DataStruct envelope) {
                    if (envelope.get(SOAP_SCHEMA_HEADER_FIELD) instanceof DataStruct header) {
                        if (header.get(SOAP_SCHEMA_HEADER_ELEMENTS_FIELD) instanceof DataList headerElements) {
                            convertHeader(result.getSOAPHeader(), headerElements);
                        }
                    }
                    var soapBody = result.getSOAPBody();
                    if (envelope.get(SOAP_SCHEMA_BODY_FIELD) instanceof DataStruct body) {
                        body.getIfPresent(SOAP_SCHEMA_ENCODING_STYLE_FIELD, DataString.class, style -> soapBody.setEncodingStyle(style.value()));

                        body.getIfPresent(SOAP_SCHEMA_BODY_ELEMENTS_FIELD, DataList.class, bodyElements -> {
                            for (var bodyElement : bodyElements) {
                                if (bodyElement instanceof DataStruct bodyElementStruct) {
                                    for (var bodyElementEntry : bodyElementStruct.entrySet()) {
                                        SOAPBodyElement soapBodyElement = soapBody.addBodyElement(new QName(bodyElementEntry.getKey()));
                                        addChildToElement(soapBodyElement, bodyElementEntry.getValue());
                                    }
                                }
                            }
                        });

                        body.getIfPresent(SOAP_SCHEMA_FAULT_FIELD, DataStruct.class, fault -> {
                            var soapFault = soapBody.addFault();
                            fault.getIfPresent(SOAP_SCHEMA_FAULT_ACTOR_FIELD, DataString.class, actor -> soapFault.setFaultActor(actor.value()));
                            fault.getIfPresent(SOAP_SCHEMA_FAULT_CODE_FIELD, DataString.class, code -> soapFault.setFaultCode(code.value()));
                            fault.getIfPresent(SOAP_SCHEMA_FAULT_STRING_FIELD, DataString.class, str -> soapFault.setFaultString(str.value()));
                            fault.getIfPresent(SOAP_SCHEMA_FAULT_DETAIL_FIELD, DataList.class, detail -> {
                                var soapDetail = soapFault.addDetail();
                                for (var faultDetail : detail) {
                                    if (faultDetail instanceof DataStruct faultDetailStruct) {
                                        var soapDetailEntry = soapDetail.addDetailEntry(convertQName(faultDetailStruct.getAs(SOAP_SCHEMA_QNAME_FIELD, DataStruct.class)));
                                        var detailValue = faultDetailStruct.getAs(SOAP_SCHEMA_FAULT_DETAIL_VALUE_FIELD, DataString.class);
                                        if (detailValue != null) {
                                            soapDetailEntry.setValue(detailValue.value());
                                        }
                                    }
                                }
                            });
                        });
                    }
                }
            }
            return result;
        } catch (SOAPException e) {
            throw FatalError.dataError("Could not convert DataObject to SOAP Message", e);
        }
    }

    private void addChildToElement(SOAPElement element, DataObject value) throws SOAPException {
        if (value instanceof DataList list) {
            for (var listElement : list) {
                addChildToElement(element, listElement);
            }
        }
        if (value instanceof DataStruct struct) {
            for (var structEntry : struct.entrySet()) {
                var childElement = element.addChildElement(new QName(structEntry.getKey()));
                addChildToElement(childElement, structEntry.getValue());
            }
        }
        if (value instanceof DataString str) {
            element.setTextContent(str.value());
        }
    }

    private void convertHeader(SOAPHeader soapHeader, DataList headerElements) throws
            SOAPException {
        for (var he : headerElements) {
            if (he instanceof DataStruct headerElement) {
                var qname = convertQName(headerElement.getAs(SOAP_SCHEMA_QNAME_FIELD, DataStruct.class));
                var soapHeaderElement = soapHeader.addHeaderElement(qname);
                headerElement.getIfPresent(SOAP_SCHEMA_ACTOR_FIELD, DataString.class, actor -> soapHeaderElement.setActor(actor.value()));
                headerElement.getIfPresent(SOAP_SCHEMA_ROLE_FIELD, DataString.class, role -> soapHeaderElement.setRole(role.value()));
                headerElement.getIfPresent(SOAP_SCHEMA_MUST_UNDERSTAND_FIELD, DataBoolean.class, mu -> soapHeaderElement.setMustUnderstand(mu.value()));
                headerElement.getIfPresent(SOAP_SCHEMA_RELAY_FIELD, DataBoolean.class, relay -> soapHeaderElement.setRelay(relay.value()));
            }
        }
    }

    private QName convertQName(DataStruct qname) {
        if (qname == null)
            throw FatalError.dataError("Can not convert empty DataObject to QName");
        var localPart = qname.getAs(SOAP_SCHEMA_QNAME_LOCAL_PART_FIELD, DataString.class);
        if (localPart == null)
            throw FatalError.dataError("Can not convert DataObject to QName, since the \"localPart\" is empty.");
        var namespaceURI = qname.getAs(SOAP_SCHEMA_QNAME_NAMESPACE_URI_FIELD, DataString.class);
        var prefix = qname.getAs(SOAP_SCHEMA_QNAME_PREFIX_FIELD, DataString.class);

        if (prefix == null)
            return new QName(namespaceURI != null ? namespaceURI.value() : null, localPart.value());
        return new QName(namespaceURI != null ? namespaceURI.value() : null, localPart.value(), prefix.value());
    }

    private void setSoapBodyElementValue(SOAPBodyElement soapBodyElement, DataObject value) throws
            SOAPException {
        if (value instanceof DataString str) {
            value = xmlMapper.toDataObject(str.value());
        }
        if (value instanceof DataStruct bodyValue) {
            setSoapBodyElementValueInternal(soapBodyElement, bodyValue);
        }
    }

    private void setSoapBodyElementValueInternal(SOAPElement soapElement, DataObject value) throws
            SOAPException {
        if (value instanceof DataStruct struct) {
            for (var entry : struct.entrySet()) {
                var child = soapElement.addChildElement(entry.getKey());
                setSoapBodyElementValueInternal(child, entry.getValue());
            }
        }
        if (value instanceof DataString str) {
            soapElement.setValue(str.value());
        }
    }
}
