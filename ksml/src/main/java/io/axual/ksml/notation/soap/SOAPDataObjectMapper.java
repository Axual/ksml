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

import javax.xml.namespace.QName;

import io.axual.ksml.data.mapper.DataObjectMapper;
import io.axual.ksml.data.object.DataBoolean;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.exception.KSMLDataException;
import io.axual.ksml.execution.FatalError;
import jakarta.xml.soap.Detail;
import jakarta.xml.soap.MessageFactory;
import jakarta.xml.soap.SOAPEnvelope;
import jakarta.xml.soap.SOAPException;
import jakarta.xml.soap.SOAPFault;
import jakarta.xml.soap.SOAPHeader;
import jakarta.xml.soap.SOAPHeaderElement;
import jakarta.xml.soap.SOAPMessage;

public class SOAPDataObjectMapper implements DataObjectMapper {
    public static final String SOAP_ACTOR = "actor";
    public static final String SOAP_BODY = "body";
    public static final String SOAP_BODY_ELEMENTS = "elements";
    public static final String SOAP_BODY_ELEMENT_VALUE = "value";
    public static final String SOAP_ENCODING_STYLE = "encodingStyle";
    public static final String SOAP_ENVELOPE = "envelope";
    public static final String SOAP_FAULT = "fault";
    public static final String SOAP_FAULT_CODE = "faultcode";
    public static final String SOAP_FAULT_STRING = "faultstring";
    public static final String SOAP_FAULT_ACTOR = "faultactor";
    public static final String SOAP_FAULT_DETAIL = "detail";
    public static final String SOAP_HEADER = "header";
    public static final String SOAP_HEADER_ELEMENTS = "elements";
    public static final String SOAP_HEADER_LOCALNAME = "localName";
    public static final String SOAP_HEADER_NAMESPACE_URI = "namespace";
    public static final String SOAP_MUST_UNDERSTAND = "mustUnderstand";
    public static final String SOAP_RELAY = "relay";
    public static final String SOAP_ROLE = "role";
    public static final String SOAP_QNAME = "qname";
    public static final String SOAP_QNAME_LOCAL_PART = "localPart";
    public static final String SOAP_QNAME_NAMESPACE_URI = "namespaceURI";
    public static final String SOAP_QNAME_PREFIX = "prefix";

    private final MessageFactory messageFactory;

    public SOAPDataObjectMapper() {
        try {
            messageFactory = MessageFactory.newInstance();
        } catch (SOAPException e) {
            throw FatalError.executionError("Could not create SOAP Message Factory", e);
        }
    }

    @Override
    public DataObject toDataObject(DataType expected, Object value) {
        if (value instanceof SOAPMessage val) return convertMessage(val);
        return null;
    }

    private DataObject convertMessage(SOAPMessage message) {
        var result = new DataStruct();

        try {
            // Convert the envelope
            result.put(SOAP_ENVELOPE, convertEnvelope(message.getSOAPPart().getEnvelope()));
        } catch (SOAPException e) {
            throw FatalError.reportAndExit(new KSMLDataException("Could not convert SOAP Part to DataObject", e));
        }

        return result;
    }

    private DataObject convertEnvelope(SOAPEnvelope envelope) throws SOAPException {
        var result = new DataStruct(null);

        // Convert the SOAP header
        if (envelope.getHeader() != null) {
            result.put(SOAP_HEADER, convertHeader(envelope.getHeader()));
        }

        // Convert body
        if (envelope.getBody() != null) {
            var body = new DataStruct(null);
            result.put(SOAP_BODY, body);
            body.putIfNotNull(SOAP_ENCODING_STYLE, DataString.from(envelope.getBody().getEncodingStyle()));

            // Convert body elements
            var bodyElements = new DataList(null);
            body.put(SOAP_BODY_ELEMENTS, bodyElements);
            envelope.getBody().getChildElements().forEachRemaining(element -> bodyElements.addIfNotNull(DataString.from(element.getValue())));
        }

        // Convert fault
        if (envelope.getBody().hasFault())
            result.put(SOAP_FAULT, convertFault(envelope.getBody().getFault()));

        return result;
    }

    private DataObject convertFault(SOAPFault fault) {
        var result = new DataStruct(null);
        result.put(SOAP_QNAME, convertQName(fault.getElementQName()));
        result.putIfNotNull(SOAP_FAULT_CODE, DataString.from(fault.getFaultCode()));
        result.putIfNotNull(SOAP_FAULT_STRING, DataString.from(fault.getFaultString()));
        result.putIfNotNull(SOAP_FAULT_ACTOR, DataString.from(fault.getFaultActor()));
        result.putIfNotNull(SOAP_FAULT_DETAIL, convertFaultDetail(fault.getDetail()));
        return result;
    }

    private DataObject convertFaultDetail(Detail detail) {
        if (detail == null) return null;

        var detailEntries = new DataList(null);
        detail.getDetailEntries().forEachRemaining(entry -> {
            var detailEntry = new DataStruct(null);
            var content = DataString.from(entry.getValue());
            detailEntry.put(SOAP_QNAME, convertQName(entry.getElementQName()));
            detailEntry.put(SOAP_FAULT_DETAIL, content);
            detailEntries.add(detailEntry);
        });

        return detailEntries.size() > 0 ? detailEntries : null;
    }

    private DataObject convertHeader(SOAPHeader header) throws SOAPException {
        if (header == null) return null;

        // Convert all header elements
        var result = new DataStruct(null);

        var headerElements = new DataList(null);
        result.put(SOAP_HEADER_ELEMENTS, headerElements);
        header.extractAllHeaderElements().forEachRemaining(element -> headerElements.addIfNotNull(convertHeaderElement(element)));

        return result;
    }

    private DataObject convertHeaderElement(SOAPHeaderElement element) {
        if (element == null) return null;

        var result = new DataStruct(null);
        result.put(SOAP_QNAME, convertQName(element.getElementQName()));
        result.putIfNotNull(SOAP_ACTOR, DataString.from(element.getActor()));
        result.putIfNotNull(SOAP_ROLE, DataString.from(element.getRole()));
        result.put(SOAP_MUST_UNDERSTAND, new DataBoolean(element.getMustUnderstand()));
        result.put(SOAP_RELAY, new DataBoolean(element.getRelay()));

        return result;
    }

    private DataObject convertQName(QName qname) {
        if (qname == null) return null;
        var result = new DataStruct();
        result.putIfNotNull(SOAP_QNAME_LOCAL_PART, DataString.from(qname.getLocalPart()));
        result.putIfNotNull(SOAP_QNAME_NAMESPACE_URI, DataString.from(qname.getNamespaceURI()));
        result.putIfNotNull(SOAP_QNAME_PREFIX, DataString.from(qname.getPrefix()));
        return result;
    }

    @Override
    public SOAPMessage fromDataObject(DataObject value) {
        try {
            var result = messageFactory.createMessage();
            if (value instanceof DataStruct message) {
                if (message.get(SOAP_ENVELOPE) instanceof DataStruct envelope) {
                    if (envelope.get(SOAP_HEADER) instanceof DataStruct header) {
                        if (header.get(SOAP_HEADER_ELEMENTS) instanceof DataList headerElements) {
                            convertHeader(result.getSOAPHeader(), headerElements);
                        }
                    }
                    var soapBody = result.getSOAPBody();
                    if (envelope.get(SOAP_BODY) instanceof DataStruct body) {
                        body.getIfPresent(SOAP_ENCODING_STYLE, DataString.class, style -> soapBody.setEncodingStyle(style.value()));

                        body.getIfPresent(SOAP_BODY_ELEMENTS, DataList.class, bodyElements -> {
                            for (var bodyElement : bodyElements) {
                                if (bodyElement instanceof DataStruct bodyElementStruct) {
                                    var qname = convertQName(bodyElementStruct.getAs(SOAP_QNAME, DataStruct.class));
                                    var bodyElementValue = bodyElementStruct.getAs(SOAP_BODY_ELEMENT_VALUE, DataString.class);
                                    if (bodyElementValue != null) {
                                        var soapBodyElement = soapBody.addBodyElement(qname);
                                        soapBodyElement.setValue(bodyElementValue.value());
                                    }
                                }
                            }
                        });

                        body.getIfPresent(SOAP_FAULT, DataStruct.class, fault -> {
                            var soapFault = soapBody.addFault();
                            fault.getIfPresent(SOAP_FAULT_ACTOR, DataString.class, actor -> soapFault.setFaultActor(actor.value()));
                            fault.getIfPresent(SOAP_FAULT_CODE, DataString.class, code -> soapFault.setFaultCode(code.value()));
                            fault.getIfPresent(SOAP_FAULT_STRING, DataString.class, str -> soapFault.setFaultString(str.value()));
                            fault.getIfPresent(SOAP_FAULT_DETAIL, DataList.class, detail -> {
                                var soapDetail = soapFault.addDetail();
                                for (var faultDetail : detail) {
                                    if (faultDetail instanceof DataStruct faultDetailStruct) {
                                        var soapDetailEntry = soapDetail.addDetailEntry(convertQName(faultDetailStruct.getAs(SOAP_QNAME, DataStruct.class)));
                                        var detailValue = faultDetailStruct.getAs(SOAP_FAULT_DETAIL, DataString.class);
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

    private void convertHeader(SOAPHeader soapHeader, DataList headerElements) throws SOAPException {
        for (var he : headerElements) {
            if (he instanceof DataStruct headerElement) {
                var qname = convertQName(headerElement.getAs(SOAP_QNAME, DataStruct.class));
                var ln = headerElement.get(SOAP_HEADER_LOCALNAME);
                if (!(ln instanceof DataString localName))
                    throw FatalError.dataError("Header element of SOAP Message does not contain a localname");
                var ns = headerElement.get(SOAP_HEADER_NAMESPACE_URI);
                var namespace = ns instanceof DataString ns2 ? ns2.value() : null;
                var soapHeaderElement = soapHeader.addHeaderElement(new QName(namespace, localName.value()));

                headerElement.getIfPresent(SOAP_ACTOR, DataString.class, actor -> soapHeaderElement.setActor(actor.value()));
                headerElement.getIfPresent(SOAP_ROLE, DataString.class, role -> soapHeaderElement.setRole(role.value()));
                headerElement.getIfPresent(SOAP_MUST_UNDERSTAND, DataBoolean.class, mu -> soapHeaderElement.setMustUnderstand(mu.value()));
                headerElement.getIfPresent(SOAP_RELAY, DataBoolean.class, relay -> soapHeaderElement.setRelay(relay.value()));
            }
        }
    }

    private QName convertQName(DataStruct qname) {
        if (qname == null)
            throw FatalError.dataError("Can not convert empty DataObject to QName");
        var localPart = qname.getAs(SOAP_QNAME_LOCAL_PART, DataString.class);
        if (localPart == null)
            throw FatalError.dataError("Can not convert DataObject to QName, since the \"localPart\" is empty.");
        var namespaceURI = qname.getAs(SOAP_QNAME_NAMESPACE_URI, DataString.class);
        var prefix = qname.getAs(SOAP_QNAME_PREFIX, DataString.class);

        if (prefix == null)
            return new QName(namespaceURI != null ? namespaceURI.value() : null, localPart.value());
        return new QName(namespaceURI != null ? namespaceURI.value() : null, localPart.value(), prefix.value());
    }
}
