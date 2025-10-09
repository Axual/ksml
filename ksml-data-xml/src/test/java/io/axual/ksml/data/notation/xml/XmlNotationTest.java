package io.axual.ksml.data.notation.xml;

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

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.base.BaseNotation;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.MapType;
import io.axual.ksml.data.type.SimpleType;
import io.axual.ksml.data.type.StructType;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link XmlNotation} ensuring correct defaults, serde selection for supported types,
 * and proper integration with KSML's notation system.
 *
 * <p>These tests verify that users can specify 'xml' as a data format in KSML definitions
 * to handle XML data with XSD schemas.</p>
 */
@DisplayName("XmlNotation - defaults, serde selection, and wiring")
class XmlNotationTest {

    @Test
    @DisplayName("Default properties: name, extension, default type")
    void basicProperties() {
        // Given: a standard XML notation context
        var context = new NotationContext(XmlNotation.NOTATION_NAME);

        // When: constructing XmlNotation
        var notation = new XmlNotation(context);

        // Then: verify defaults using chained assertions
        assertThat(notation)
                .returns("xml", BaseNotation::name)
                .returns(".xsd", BaseNotation::filenameExtension)
                .returns(XmlNotation.DEFAULT_TYPE, XmlNotation::defaultType);

        // Converter should be XmlDataObjectConverter
        assertThat(notation.converter()).isInstanceOf(XmlDataObjectConverter.class);

        // Schema parser should be XmlSchemaParser
        assertThat(notation.schemaParser()).isInstanceOf(XmlSchemaParser.class);
    }

    @Test
    @DisplayName("Name includes vendor prefix when provided by context")
    void nameWithVendor() {
        // Given: context with vendor
        var context = new NotationContext(XmlNotation.NOTATION_NAME, "vendorX");

        // When: constructing notation
        var notation = new XmlNotation(context);

        // Then: name should include vendor prefix
        assertThat(notation.name()).isEqualTo("vendorX_xml");
    }

    @Test
    @DisplayName("Serde is provided for MapType and StructType")
    void serdeForSupportedTypes() {
        // Given: XML notation
        var notation = new XmlNotation(new NotationContext(XmlNotation.NOTATION_NAME));

        // When/Then: verify serde creation for supported types
        var softly = new SoftAssertions();

        // MapType is supported
        try (var serde = notation.serde(new MapType(), false)) {
            softly.assertThat(serde)
                    .as("MapType should have serde")
                    .isNotNull();
        }

        // StructType is supported
        try (var serde = notation.serde(new StructType(), false)) {
            softly.assertThat(serde)
                    .as("StructType should have serde")
                    .isNotNull();
        }

        // DEFAULT_TYPE (StructType) is supported
        try (var serde = notation.serde(XmlNotation.DEFAULT_TYPE, false)) {
            softly.assertThat(serde)
                    .as("DEFAULT_TYPE should have serde")
                    .isNotNull();
        }

        softly.assertAll();
    }

    @Test
    @DisplayName("Throws exception for unsupported types (not Map or Struct)")
    @SuppressWarnings("resource") // Serde is never created because exception is thrown
    void unsupportedTypeThrowsException() {
        // Given: XML notation
        var notation = new XmlNotation(new NotationContext(XmlNotation.NOTATION_NAME));

        // When/Then: trying to create serde for unsupported types should throw
        var softly = new SoftAssertions();

        // SimpleType is not supported
        softly.assertThatThrownBy(() -> notation.serde(new SimpleType(String.class, "string"), false))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("serde");

        // ListType is not supported
        softly.assertThatThrownBy(() -> notation.serde(new ListType(), false))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("serde");

        softly.assertAll();
    }

    @Test
    @DisplayName("File extension is .xsd for XML Schema definitions")
    void filenameExtension() {
        // Given: XML notation
        var notation = new XmlNotation(new NotationContext(XmlNotation.NOTATION_NAME));

        // Then: extension should be .xsd
        assertThat(notation.filenameExtension()).isEqualTo(".xsd");
    }

    @Test
    @DisplayName("DEFAULT_TYPE is StructType")
    void defaultTypeIsStruct() {
        // Given: XML notation's DEFAULT_TYPE
        var defaultType = XmlNotation.DEFAULT_TYPE;

        // Then: should be a StructType
        assertThat(defaultType).isInstanceOf(StructType.class);
    }

    @Test
    @DisplayName("Serde works for both key and value serialization")
    void keyAndValueSerde() {
        // Given: XML notation
        var notation = new XmlNotation(new NotationContext(XmlNotation.NOTATION_NAME));
        var structType = new StructType();

        // When/Then: create serdes for both key and value
        var softly = new SoftAssertions();

        try (var serde = notation.serde(structType, true)) {
            softly.assertThat(serde)
                    .as("Key serde should be created")
                    .isNotNull();
        }

        try (var serde = notation.serde(structType, false)) {
            softly.assertThat(serde)
                    .as("Value serde should be created")
                    .isNotNull();
        }

        softly.assertAll();
    }

    @Test
    @DisplayName("MapType serde can be created for generic XML structures")
    void mapTypeSerde() {
        // Given: XML notation
        var notation = new XmlNotation(new NotationContext(XmlNotation.NOTATION_NAME));
        var mapType = new MapType();

        // When: creating serde for MapType
        try (var serde = notation.serde(mapType, false)) {
            // Then: should create serde successfully
            assertThat(serde).isNotNull();
        }
    }
}
