package io.axual.ksml.data.notation.binary;

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

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.base.BaseNotation;
import io.axual.ksml.data.serde.ByteSerde;
import io.axual.ksml.data.serde.NullSerde;
import io.axual.ksml.data.serde.SerdeSupplier;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.SimpleType;
import io.axual.ksml.data.value.Null;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link BinaryNotation} ensuring correct defaults, serde selection for supported types,
 * and proper integration with KSML's notation system.
 *
 * <p>These tests verify that users can specify 'binary' as a data format in KSML definitions
 * to handle raw byte arrays and primitive types.</p>
 */
@DisplayName("BinaryNotation - defaults, serde selection, and wiring")
class BinaryNotationTest {

    @Test
    @DisplayName("Default properties: name, extension, default type")
    void basicProperties() {
        // Given: a standard binary notation context
        var context = new NotationContext(BinaryNotation.NOTATION_NAME);

        // When: constructing BinaryNotation
        var notation = new BinaryNotation(context, null);

        // Then: verify defaults using chained assertions
        assertThat(notation)
                .returns("binary", BaseNotation::name)
                .returns(null, BaseNotation::filenameExtension)  // Binary notation doesn't define a filename extension
                .returns(BinaryNotation.DEFAULT_TYPE, BinaryNotation::defaultType);

        // Converter should be null (binary doesn't use converter)
        assertThat(notation.converter()).isNull();

        // Schema parser should be null (binary doesn't use schema)
        assertThat(notation.schemaParser()).isNull();
    }

    @Test
    @DisplayName("Name includes vendor prefix when provided by context")
    void nameWithVendor() {
        // Given: context with vendor
        var context = new NotationContext(BinaryNotation.NOTATION_NAME, "vendorX");

        // When: constructing notation
        var notation = new BinaryNotation(context, null);

        // Then: name should include vendor prefix
        assertThat(notation.name()).isEqualTo("vendorX_binary");
    }

    @Test
    @DisplayName("Serde is provided for Byte and Null simple types")
    void serdeForSimpleTypes() {
        // Given: Binary notation
        var notation = new BinaryNotation(new NotationContext(BinaryNotation.NOTATION_NAME), null);

        // When/Then: verify serde creation for supported simple types
        var softly = new SoftAssertions();

        // Byte type should use ByteSerde
        var byteType = new SimpleType(Byte.class, "byte");
        try (var serde = notation.serde(byteType, false)) {
            softly.assertThat(serde)
                    .as("Byte type should have ByteSerde")
                    .isInstanceOf(ByteSerde.class);
        }

        // Null type should use NullSerde
        var nullType = new SimpleType(Null.class, "null");
        try (var serde = notation.serde(nullType, false)) {
            softly.assertThat(serde)
                    .as("Null type should have NullSerde")
                    .isInstanceOf(NullSerde.class);
        }

        // Integer type should use BinaryNotation.BinarySerde wrapper
        var intType = new SimpleType(Integer.class, "int");
        try (var serde = notation.serde(intType, false)) {
            softly.assertThat(serde)
                    .as("Integer type should have serde")
                    .isNotNull();
        }

        // Long type should use BinaryNotation.BinarySerde wrapper
        var longType = new SimpleType(Long.class, "long");
        try (var serde = notation.serde(longType, false)) {
            softly.assertThat(serde)
                    .as("Long type should have serde")
                    .isNotNull();
        }

        // String type should use BinaryNotation.BinarySerde wrapper
        var stringType = new SimpleType(String.class, "string");
        try (var serde = notation.serde(stringType, false)) {
            softly.assertThat(serde)
                    .as("String type should have serde")
                    .isNotNull();
        }

        softly.assertAll();
    }

    @Test
    @DisplayName("Throws exception for complex types without SerdeSupplier")
    void unsupportedComplexTypeWithoutSupplierThrowsException() {
        // Given: Binary notation without complex type serde supplier
        var notation = new BinaryNotation(new NotationContext(BinaryNotation.NOTATION_NAME), null);

        // When/Then: trying to create serde for complex type should throw
        var complexType = new ListType();

        assertThatThrownBy(() -> notation.serde(complexType, false))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("serde");
    }

    @Test
    @DisplayName("Delegates complex types to SerdeSupplier when provided")
    void complexTypeWithSupplier() {
        // Given: Binary notation with a mock SerdeSupplier
        var context = new NotationContext(BinaryNotation.NOTATION_NAME);
        var mockSupplier = (SerdeSupplier) (type, isKey) -> new Serde<>() {
            @Override
            public Serializer<Object> serializer() {
                return (topic, data) -> new byte[0];
            }

            @Override
            public Deserializer<Object> deserializer() {
                return (topic, data) -> new Object();
            }
        };
        var notation = new BinaryNotation(context, mockSupplier);

        // When: requesting serde for complex type
        var complexType = new ListType();
        try (var serde = notation.serde(complexType, false)) {
            // Then: should return serde from supplier
            assertThat(serde).isNotNull();
        }
    }

    @Test
    @DisplayName("File extension is null (binary data doesn't have a standard extension in KSML)")
    void filenameExtension() {
        // Given: Binary notation
        var notation = new BinaryNotation(new NotationContext(BinaryNotation.NOTATION_NAME), null);

        // Then: extension should be null (not defined for binary notation)
        assertThat(notation.filenameExtension()).isNull();
    }

    @Test
    @DisplayName("DEFAULT_TYPE is UNKNOWN")
    void defaultTypeIsUnknown() {
        // Given: Binary notation's DEFAULT_TYPE
        var defaultType = BinaryNotation.DEFAULT_TYPE;

        // Then: should be DataType.UNKNOWN
        assertThat(defaultType).isEqualTo(DataType.UNKNOWN);
    }

    @Test
    @DisplayName("Serde works for both key and value serialization")
    void keyAndValueSerde() {
        // Given: Binary notation
        var notation = new BinaryNotation(new NotationContext(BinaryNotation.NOTATION_NAME), null);
        var byteType = new SimpleType(Byte.class, "byte");

        // When/Then: create serdes for both key and value
        var softly = new SoftAssertions();

        try (var serde = notation.serde(byteType, true)) {
            softly.assertThat(serde)
                    .as("Key serde should be created")
                    .isInstanceOf(ByteSerde.class);
        }

        try (var serde = notation.serde(byteType, false)) {
            softly.assertThat(serde)
                    .as("Value serde should be created")
                    .isInstanceOf(ByteSerde.class);
        }

        softly.assertAll();
    }
}
