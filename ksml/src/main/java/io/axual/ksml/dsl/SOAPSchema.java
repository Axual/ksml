package io.axual.ksml.dsl;

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

import java.util.ArrayList;

import io.axual.ksml.data.schema.DataField;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.ListSchema;
import io.axual.ksml.data.schema.StructSchema;

import static io.axual.ksml.dsl.DataSchemaConstants.DATA_SCHEMA_KSML_NAMESPACE;
import static io.axual.ksml.data.schema.DataSchema.Type.BOOLEAN;
import static io.axual.ksml.data.schema.DataSchema.Type.STRING;

public class SOAPSchema {
    private SOAPSchema() {
    }

    // Public constants are the field names
    public static final StructSchema SOAP_QNAME_SCHEMA = generateQNameSchema();
    public static final StructSchema SOAP_HEADER_ELEMENT_SCHEMA = generateHeaderElementSchema();
    public static final StructSchema SOAP_HEADER_SCHEMA = generateHeaderSchema();
    public static final StructSchema SOAP_FAULT_DETAIL_SCHEMA = generateFaultDetailSchema();
    public static final StructSchema SOAP_FAULT_SCHEMA = generateFaultSchema();
    public static final String SOAP_MESSAGE_SCHEMA_NAME = "SOAPMessage";
    public static final String SOAP_ENVELOPE_SCHEMA_NAME = "SOAPEnvelope";
    public static final String SOAP_HEADER_SCHEMA_NAME = "SOAPHeader";
    public static final String SOAP_HEADER_ELEMENT_SCHEMA_NAME = "SOAPHeaderElement";
    public static final String SOAP_BODY_SCHEMA_NAME = "SOAPBody";
    public static final String SOAP_FAULT_SCHEMA_NAME = "SOAPFault";
    public static final String SOAP_FAULT_DETAIL_SCHEMA_NAME = "SOAPFaultDetail";
    public static final String SOAP_QNAME_SCHEMA_NAME = "SOAPQName";
    public static final String SOAP_SCHEMA_ACTOR_FIELD = "actor";
    public static final String SOAP_SCHEMA_BODY_FIELD = "body";
    public static final String SOAP_SCHEMA_BODY_ELEMENTS_FIELD = "elements";
    public static final String SOAP_SCHEMA_ENCODING_STYLE_FIELD = "encodingStyle";
    public static final String SOAP_SCHEMA_ENVELOPE_FIELD = "envelope";
    public static final String SOAP_SCHEMA_FAULT_FIELD = "fault";
    public static final String SOAP_SCHEMA_FAULT_CODE_FIELD = "faultcode";
    public static final String SOAP_SCHEMA_FAULT_STRING_FIELD = "faultstring";
    public static final String SOAP_SCHEMA_FAULT_ACTOR_FIELD = "faultactor";
    public static final String SOAP_SCHEMA_FAULT_DETAIL_FIELD = "detail";
    public static final String SOAP_SCHEMA_FAULT_DETAIL_VALUE_FIELD = "value";
    public static final String SOAP_SCHEMA_HEADER_FIELD = "header";
    public static final String SOAP_SCHEMA_HEADER_ELEMENTS_FIELD = "elements";
    public static final String SOAP_SCHEMA_HEADER_LOCAL_NAME_FIELD = "localName";
    public static final String SOAP_SCHEMA_HEADER_NAMESPACE_URI_FIELD = "namespace";
    public static final String SOAP_SCHEMA_MUST_UNDERSTAND_FIELD = "mustUnderstand";
    public static final String SOAP_SCHEMA_RELAY_FIELD = "relay";
    public static final String SOAP_SCHEMA_ROLE_FIELD = "role";
    public static final String SOAP_SCHEMA_QNAME_FIELD = "qname";
    public static final String SOAP_SCHEMA_QNAME_LOCAL_PART_FIELD = "localPart";
    public static final String SOAP_SCHEMA_QNAME_NAMESPACE_URI_FIELD = "namespaceURI";
    public static final String SOAP_SCHEMA_QNAME_PREFIX_FIELD = "prefix";

    // Private constants are the schema descriptions and doc fields
    public static final String SOAP_MESSAGE_SCHEMA_DOC = "SOAP message";
    private static final String SOAP_ENVELOPE_SCHEMA_DOC = "SOAP envelope";
    private static final String SOAP_HEADER_SCHEMA_DOC = "SOAP header";
    private static final String SOAP_HEADER_ELEMENT_SCHEMA_DOC = "SOAP header element";
    private static final String SOAP_BODY_SCHEMA_DOC = "SOAP body";
    private static final String SOAP_FAULT_SCHEMA_DOC = "SOAP fault";
    private static final String SOAP_FAULT_DETAIL_SCHEMA_DOC = "SOAP fault detail";
    private static final String SOAP_QNAME_SCHEMA_DOC = "SOAP QName";
    private static final String SOAP_SCHEMA_QNAME_LOCAL_PART_DOC = "localPart";
    private static final String SOAP_SCHEMA_QNAME_NAMESPACE_URI_DOC = "namespace URI";
    private static final String SOAP_SCHEMA_QNAME_PREFIX_DOC = "prefix";
    private static final String SOAP_SCHEMA_ACTOR_DOC = "Actor";
    private static final String SOAP_SCHEMA_ROLE_DOC = "Role";
    private static final String SOAP_SCHEMA_MUST_UNDERSTAND_DOC = "Must understand";
    private static final String SOAP_SCHEMA_RELAY_DOC = "Relay";
    private static final String SOAP_SCHEMA_HEADER_ELEMENTS_DOC = "Header elements";
    private static final String SOAP_SCHEMA_ENCODING_STYLE_DOC = "Encoding style";
    private static final String SOAP_SCHEMA_BODY_ELEMENTS_DOC = "Body elements";
    private static final String SOAP_SCHEMA_FAULT_ACTOR_DOC = "Fault actor";
    private static final String SOAP_SCHEMA_FAULT_CODE_DOC = "Fault code";
    private static final String SOAP_SCHEMA_FAULT_STRING_DOC = "Fault string";
    private static final String SOAP_SCHEMA_FAULT_DETAIL_DOC = "Fault details";
    private static final String SOAP_SCHEMA_FAULT_DETAIL_VALUE_DOC = "Fault details value";

    private static StructSchema generateQNameSchema() {
        var qnameFields = new ArrayList<DataField>();
        qnameFields.add(new DataField(SOAP_SCHEMA_QNAME_LOCAL_PART_FIELD, DataSchema.create(STRING), SOAP_SCHEMA_QNAME_LOCAL_PART_DOC, null, DataField.Order.ASCENDING));
        qnameFields.add(new DataField(SOAP_SCHEMA_QNAME_NAMESPACE_URI_FIELD, DataSchema.create(STRING), SOAP_SCHEMA_QNAME_NAMESPACE_URI_DOC, null, DataField.Order.ASCENDING));
        qnameFields.add(new DataField(SOAP_SCHEMA_QNAME_PREFIX_FIELD, DataSchema.create(STRING), SOAP_SCHEMA_QNAME_PREFIX_DOC, null, DataField.Order.ASCENDING));
        return new StructSchema(DATA_SCHEMA_KSML_NAMESPACE, SOAP_QNAME_SCHEMA_NAME, SOAP_QNAME_SCHEMA_DOC, qnameFields);
    }

    private static StructSchema generateHeaderElementSchema() {
        var elementFields = new ArrayList<DataField>();
        elementFields.add(new DataField(SOAP_SCHEMA_QNAME_FIELD, SOAP_QNAME_SCHEMA, SOAP_QNAME_SCHEMA.doc(), null, DataField.Order.ASCENDING));
        elementFields.add(new DataField(SOAP_SCHEMA_ACTOR_FIELD, DataSchema.create(STRING), SOAP_SCHEMA_ACTOR_DOC, null, DataField.Order.ASCENDING));
        elementFields.add(new DataField(SOAP_SCHEMA_ROLE_FIELD, DataSchema.create(STRING), SOAP_SCHEMA_ROLE_DOC, null, DataField.Order.ASCENDING));
        elementFields.add(new DataField(SOAP_SCHEMA_MUST_UNDERSTAND_FIELD, DataSchema.create(BOOLEAN), SOAP_SCHEMA_MUST_UNDERSTAND_DOC, null, DataField.Order.ASCENDING));
        elementFields.add(new DataField(SOAP_SCHEMA_RELAY_FIELD, DataSchema.create(BOOLEAN), SOAP_SCHEMA_RELAY_DOC, null, DataField.Order.ASCENDING));
        return new StructSchema(DATA_SCHEMA_KSML_NAMESPACE, SOAP_HEADER_ELEMENT_SCHEMA_NAME, SOAP_HEADER_ELEMENT_SCHEMA_DOC, elementFields);
    }

    private static StructSchema generateHeaderSchema() {
        var headerFields = new ArrayList<DataField>();
        headerFields.add(new DataField(SOAP_SCHEMA_HEADER_ELEMENTS_FIELD, new ListSchema(SOAP_HEADER_ELEMENT_SCHEMA), SOAP_SCHEMA_HEADER_ELEMENTS_DOC, null, DataField.Order.ASCENDING));
        return new StructSchema(DATA_SCHEMA_KSML_NAMESPACE, SOAP_HEADER_SCHEMA_NAME, SOAP_HEADER_SCHEMA_DOC, headerFields);
    }

    public static StructSchema generateBodySchema(DataSchema bodyElementSchema) {
        var bodyFields = new ArrayList<DataField>();
        bodyFields.add(new DataField(SOAP_SCHEMA_ENCODING_STYLE_FIELD, DataSchema.create(STRING), SOAP_SCHEMA_ENCODING_STYLE_DOC, null, DataField.Order.ASCENDING));
        bodyFields.add(new DataField(SOAP_SCHEMA_BODY_ELEMENTS_FIELD, new ListSchema(bodyElementSchema), SOAP_SCHEMA_BODY_ELEMENTS_DOC, null, DataField.Order.ASCENDING));
        return new StructSchema(DATA_SCHEMA_KSML_NAMESPACE, SOAP_BODY_SCHEMA_NAME, SOAP_BODY_SCHEMA_DOC, bodyFields);
    }

    private static StructSchema generateFaultDetailSchema() {
        var faultDetailFields = new ArrayList<DataField>();
        faultDetailFields.add(new DataField(SOAP_SCHEMA_QNAME_FIELD, SOAP_QNAME_SCHEMA, SOAP_QNAME_SCHEMA.doc(), null, DataField.Order.ASCENDING));
        faultDetailFields.add(new DataField(SOAP_SCHEMA_FAULT_DETAIL_VALUE_FIELD, DataSchema.create(STRING), SOAP_SCHEMA_FAULT_DETAIL_VALUE_DOC, null, DataField.Order.ASCENDING));
        return new StructSchema(DATA_SCHEMA_KSML_NAMESPACE, SOAP_FAULT_DETAIL_SCHEMA_NAME, SOAP_FAULT_DETAIL_SCHEMA_DOC, faultDetailFields);
    }

    private static StructSchema generateFaultSchema() {
        var faultFields = new ArrayList<DataField>();
        faultFields.add(new DataField(SOAP_SCHEMA_QNAME_FIELD, SOAP_QNAME_SCHEMA, SOAP_QNAME_SCHEMA.doc(), null, DataField.Order.ASCENDING));
        faultFields.add(new DataField(SOAP_SCHEMA_FAULT_ACTOR_FIELD, DataSchema.create(STRING), SOAP_SCHEMA_FAULT_ACTOR_DOC, null, DataField.Order.ASCENDING));
        faultFields.add(new DataField(SOAP_SCHEMA_FAULT_CODE_FIELD, DataSchema.create(STRING), SOAP_SCHEMA_FAULT_CODE_DOC, null, DataField.Order.ASCENDING));
        faultFields.add(new DataField(SOAP_SCHEMA_FAULT_STRING_FIELD, DataSchema.create(STRING), SOAP_SCHEMA_FAULT_STRING_DOC, null, DataField.Order.ASCENDING));
        faultFields.add(new DataField(SOAP_SCHEMA_FAULT_DETAIL_FIELD, new ListSchema(SOAP_FAULT_DETAIL_SCHEMA), SOAP_SCHEMA_FAULT_DETAIL_DOC, null, DataField.Order.ASCENDING));
        return new StructSchema(DATA_SCHEMA_KSML_NAMESPACE, SOAP_FAULT_SCHEMA_NAME, SOAP_FAULT_SCHEMA_DOC, faultFields);
    }

    public static StructSchema generateEnvelopeSchema(DataSchema bodyElementSchema) {
        var envelopeFields = new ArrayList<DataField>();
        var body = generateBodySchema(bodyElementSchema);
        envelopeFields.add(new DataField(SOAP_SCHEMA_HEADER_FIELD, SOAP_HEADER_SCHEMA, SOAP_HEADER_SCHEMA.doc(), null, DataField.Order.ASCENDING));
        envelopeFields.add(new DataField(SOAP_SCHEMA_BODY_FIELD, body, body.doc(), null, DataField.Order.ASCENDING));
        envelopeFields.add(new DataField(SOAP_SCHEMA_FAULT_FIELD, SOAP_FAULT_SCHEMA, SOAP_FAULT_SCHEMA.doc(), null, DataField.Order.ASCENDING));
        return new StructSchema(DATA_SCHEMA_KSML_NAMESPACE, SOAP_ENVELOPE_SCHEMA_NAME, SOAP_ENVELOPE_SCHEMA_DOC, envelopeFields);
    }

    public static StructSchema generateSOAPSchema(DataSchema bodyElementSchema) {
        var messageFields = new ArrayList<DataField>();
        var envelope = generateEnvelopeSchema(bodyElementSchema);
        messageFields.add(new DataField(SOAP_SCHEMA_ENVELOPE_FIELD, envelope, envelope.doc(), null, DataField.Order.ASCENDING));
        return new StructSchema(DATA_SCHEMA_KSML_NAMESPACE, SOAP_MESSAGE_SCHEMA_NAME, SOAP_MESSAGE_SCHEMA_DOC, messageFields);
    }
}
