package io.axual.ksml.data.notation;

/*-
 * ========================LICENSE_START=================================
 * KSML
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

import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.notation.avro.AvroDataObjectMapper;
import io.axual.ksml.data.notation.xml.XmlDataObjectMapper;
import io.axual.ksml.data.notation.xml.XmlNotation;
import io.axual.ksml.data.notation.xml.XmlSchemaMapper;
import io.axual.ksml.execution.SchemaLibrary;
import org.junit.jupiter.api.Test;

class XmlTests {
    @Test
    void schemaTest() {
//        NotationTestRunner.schemaTest("xml", new XmlSchemaMapper());
    }

    @Test
    void dataTest() {
        final var input = """
<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
           targetNamespace="http://namespace.io/address">
    <xs:element name="TestSchema">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="name" type="xs:string"/>
                <xs:element name="age" type="xs:integer"/>
                <xs:element name="address2" type="AddressSchema" />
                <xs:element name="address">
                    <xs:complexType>
                        <xs:sequence>
                            <xs:element name="street" type="xs:string"/>
                            <xs:element name="postalCode" type="xs:string"/>
                            <xs:element name="city" type="xs:string"/>
                            <xs:element name="country" type="xs:string"/>
                        </xs:sequence>
                    </xs:complexType>
                </xs:element>
                <xs:element name="shippingAddress">
                    <xs:complexType>
                        <xs:sequence>
                            <xs:element name="street" type="xs:string"/>
                            <xs:element name="postalCode" type="xs:string"/>
                            <xs:element name="city" type="xs:string"/>
                            <xs:element name="country" type="xs:string"/>
                        </xs:sequence>
                    </xs:complexType>
                </xs:element>
                <xs:element name="eyeColor"/>
                <xs:element name="luckyNumbers"/>
                <xs:element name="accountNumber"/>
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    <xs:element name="TestSchema2">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="name" type="xs:string"/>
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    <xs:complexType name="AddressSchema">
        <xs:sequence>
            <xs:element name="name" type="xs:string"/>
        </xs:sequence>
    </xs:complexType>
    <xs:complexType name="AddressSchema2">
        <xs:sequence>
            <xs:element name="name" type="xs:string"/>
        </xs:sequence>
    </xs:complexType>
</xs:schema>
                """;
        final var result = new XmlDataObjectMapper(true).toDataObject(input);
        NotationTestRunner.dataTest("xml", new AvroDataObjectMapper());
    }

    @Test
    void serdeTest() {
        final var notation = new XmlNotation("xml", new NativeDataObjectMapper());
        final var lib = new SchemaLibrary();
        lib.schemaDirectory("/Users/dizzl/dev/axual/ksml/examples");
//        final var schema = lib.getSchema(notation,"TestSchema", false);
//        NotationTestRunner.serdeTest("xml", new XmlNotation("xml", new NativeDataObjectMapper()), false);
    }
}
