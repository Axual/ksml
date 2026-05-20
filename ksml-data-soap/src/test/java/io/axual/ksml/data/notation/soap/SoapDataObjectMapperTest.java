package io.axual.ksml.data.notation.soap;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - SOAP
 * %%
 * Copyright (C) 2021 - 2026 Axual B.V.
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

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.object.DataBoolean;
import io.axual.ksml.data.object.DataBytes;
import io.axual.ksml.data.object.DataDouble;
import io.axual.ksml.data.object.DataFloat;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataShort;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.object.DataTuple;
import io.axual.ksml.data.type.DataType;
import jakarta.xml.soap.MessageFactory;
import jakarta.xml.soap.SOAPConstants;
import jakarta.xml.soap.SOAPElement;
import jakarta.xml.soap.SOAPException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.xml.namespace.QName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Unit tests for {@link SoapDataObjectMapper#addChildToElement}.
 *
 * <p>The previous implementation used three independent {@code if} statements that silently dropped
 * any DataObject that wasn't a {@code DataList}, {@code DataStruct} or {@code DataString} — a
 * numeric or boolean SOAP body field would just disappear. These tests pin down the new contract:
 * scalar primitives are stringified into the element, {@code DataNull} leaves the element empty,
 * and unsupported types throw {@link DataException}.</p>
 */
class SoapDataObjectMapperTest {

    private SoapDataObjectMapper mapper;
    private SOAPElement element;

    @BeforeEach
    void setUp() throws SOAPException {
        mapper = new SoapDataObjectMapper();
        final var msg = MessageFactory.newInstance(SOAPConstants.SOAP_1_1_PROTOCOL).createMessage();
        element = msg.getSOAPBody().addBodyElement(new QName("test"));
    }

    @Test
    @DisplayName("DataString → element text content")
    void dataString_setsTextContent() throws SOAPException {
        mapper.addChildToElement(element, new DataString("hello"));
        assertThat(element.getTextContent()).isEqualTo("hello");
    }

    @Test
    @DisplayName("DataInteger → stringified text content (previously: silent drop)")
    void dataInteger_stringified() throws SOAPException {
        mapper.addChildToElement(element, new DataInteger(42));
        assertThat(element.getTextContent()).isEqualTo("42");
    }

    @Test
    @DisplayName("DataLong → stringified text content (previously: silent drop)")
    void dataLong_stringified() throws SOAPException {
        mapper.addChildToElement(element, new DataLong(5_000_000_000L));
        assertThat(element.getTextContent()).isEqualTo("5000000000");
    }

    @Test
    @DisplayName("DataShort → stringified text content")
    void dataShort_stringified() throws SOAPException {
        mapper.addChildToElement(element, new DataShort((short) 7));
        assertThat(element.getTextContent()).isEqualTo("7");
    }

    @Test
    @DisplayName("DataDouble → stringified text content (previously: silent drop)")
    void dataDouble_stringified() throws SOAPException {
        mapper.addChildToElement(element, new DataDouble(3.14));
        assertThat(element.getTextContent()).isEqualTo("3.14");
    }

    @Test
    @DisplayName("DataFloat → stringified text content (previously: silent drop)")
    void dataFloat_stringified() throws SOAPException {
        mapper.addChildToElement(element, new DataFloat(1.5f));
        assertThat(element.getTextContent()).isEqualTo("1.5");
    }

    @Test
    @DisplayName("DataBoolean → stringified text content (previously: silent drop)")
    void dataBoolean_stringified() throws SOAPException {
        mapper.addChildToElement(element, new DataBoolean(true));
        assertThat(element.getTextContent()).isEqualTo("true");
    }

    @Test
    @DisplayName("DataBytes → byte-array toString (Java default, but non-null) (previously: silent drop)")
    void dataBytes_stringified() throws SOAPException {
        mapper.addChildToElement(element, new DataBytes(new byte[]{1, 2, 3}));
        // Default Java byte[] toString is not friendly, but the value survives — the point is that
        // the field is no longer silently dropped.
        assertThat(element.getTextContent()).isNotEmpty();
    }

    @Test
    @DisplayName("DataNull → element stays empty (no exception)")
    void dataNull_leavesElementEmpty() throws SOAPException {
        mapper.addChildToElement(element, DataNull.INSTANCE);
        assertThat(element.getTextContent()).isEmpty();
    }

    @Test
    @DisplayName("DataList of strings → recurses, last element wins on a single element")
    void dataList_recurses() throws SOAPException {
        final var list = new DataList(DataType.UNKNOWN);
        list.add(new DataString("hello"));
        mapper.addChildToElement(element, list);
        assertThat(element.getTextContent()).isEqualTo("hello");
    }

    @Test
    @DisplayName("DataStruct → adds child element per entry")
    void dataStruct_addsChildren() throws SOAPException {
        final var struct = new DataStruct();
        struct.put("greeting", new DataString("hi"));
        mapper.addChildToElement(element, struct);
        assertThat(element.getChildNodes().getLength()).isOne();
        assertThat(element.getFirstChild().getTextContent()).isEqualTo("hi");
    }

    @Test
    @DisplayName("Unsupported DataObject type (DataTuple) throws DataException")
    void unsupportedType_throws() {
        final var tuple = new DataTuple(new DataString("a"), new DataString("b"));
        assertThatCode(() -> mapper.addChildToElement(element, tuple))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("Unsupported DataObject type");
    }

    @Test
    @DisplayName("Null DataObject throws DataException (defensive)")
    void nullValue_throws() {
        assertThatCode(() -> mapper.addChildToElement(element, (DataObject) null))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("Unsupported");
    }
}
