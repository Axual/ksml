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
import io.axual.ksml.data.notation.xml.XmlDataObjectMapper;
import io.axual.ksml.data.notation.xml.XmlNotation;
import io.axual.ksml.data.notation.xml.XmlSchemaMapper;
import org.junit.jupiter.api.Test;

class XmlTests {
    @Test
    void schemaTest() {
        final var testSchema = """
<?xml version="1.0" encoding="UTF-8"?>
                <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns="io.axual.ksml.data.test"
                                       attributeFormDefault="unqualified" elementFormDefault="unqualified"
                                       targetNamespace="io.axual.ksml.data.test">
                                <xs:element name="TestSchema">
                                    <xs:annotation>
                                        <xs:documentation>Schema used for testing</xs:documentation>
                                    </xs:annotation>
                                    <xs:complexType>
                                        <xs:sequence>
                                            <xs:element name="name" type="xs:string"/>
                                            <xs:element name="age" type="xs:integer"/>
                                            <xs:element name="address" type="AddressSchema"/>
                                            <xs:element name="shippingAddress" type="AddressSchema"/>
                                            <xs:element default="BLUE" name="eyeColor" type="EyeColor"/>
                                            <xs:element name="luckyNumbers">
                                                <xs:simpleType>
                                                    <xs:list itemType="xs:long"/>
                                                </xs:simpleType>
                                            </xs:element>
                                            <xs:element name="accountNumber">
                                                <xs:simpleType>
                                                    <xs:union memberTypes="xs:long xs:string"/>
                                                </xs:simpleType>
                                            </xs:element>
                                        </xs:sequence>
                                    </xs:complexType>
                                </xs:element>
                                <xs:complexType name="AddressSchema">
                                    <xs:annotation>
                                        <xs:documentation>Bladiebla</xs:documentation>
                                    </xs:annotation>
                                    <xs:sequence>
                                        <xs:element name="street" type="xs:string"/>
                                        <xs:element name="postalCode" type="xs:string"/>
                                        <xs:element name="city" type="xs:string"/>
                                        <xs:element name="country" type="xs:string"/>
                                    </xs:sequence>
                                </xs:complexType>
                                <xs:simpleType name="EyeColor">
                                    <xs:annotation>
                                        <xs:documentation>Test</xs:documentation>
                                    </xs:annotation>
                                    <xs:restriction base="xs:string">
                                        <xs:enumeration value="UNKNOWN"/>
                                        <xs:enumeration value="BLUE"/>
                                        <xs:enumeration value="GREEN"/>
                                        <xs:enumeration value="BROWN"/>
                                        <xs:enumeration value="GREY"/>
                                    </xs:restriction>
                                </xs:simpleType>
                            </xs:schema>
                """;
        final var dataSchema = new XmlSchemaMapper().toDataSchema("dummy", "TestSchema", testSchema);
        NotationTestRunner.schemaTest("xml", new XmlSchemaMapper());
    }

    @Test
    void dataTest() {
        NotationTestRunner.dataTest("xml", new XmlDataObjectMapper(true));
    }

    @Test
    void serdeTest() {
        NotationTestRunner.serdeTest("xml", new XmlNotation("xml", new NativeDataObjectMapper()), false);
    }
}
