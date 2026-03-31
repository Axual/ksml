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

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.notation.base.BaseNotation;
import io.axual.ksml.data.serde.ByteSerde;
import io.axual.ksml.data.serde.NullSerde;
import io.axual.ksml.data.serde.SerdeSupplier;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.SimpleType;
import io.axual.ksml.data.value.Null;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

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
        // When: constructing BinaryNotation
        final var notation = new BinaryNotation();

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
    @DisplayName("Serde is provided for Byte and Null simple types")
    void serdeForSimpleTypes() {
        // Given: Binary notation
        final var notation = new BinaryNotation();

        // When/Then: verify serde creation for supported simple types
        final var softly = new SoftAssertions();

        // Byte type should use ByteSerde
        final var byteType = new SimpleType(Byte.class, "byte");
        try (var serde = notation.serde(byteType, false)) {
            softly.assertThat(serde)
                    .as("Byte type should have ByteSerde")
                    .isInstanceOf(ByteSerde.class);
        }

        // Null type should use NullSerde
        final var nullType = new SimpleType(Null.class, "null");
        try (var serde = notation.serde(nullType, false)) {
            softly.assertThat(serde)
                    .as("Null type should have NullSerde")
                    .isInstanceOf(NullSerde.class);
        }

        // Integer type should use BinaryNotation.BinarySerde wrapper
        final var intType = new SimpleType(Integer.class, "int");
        try (var serde = notation.serde(intType, false)) {
            softly.assertThat(serde)
                    .as("Integer type should have serde")
                    .isNotNull();
        }

        // Long type should use BinaryNotation.BinarySerde wrapper
        final var longType = new SimpleType(Long.class, "long");
        try (var serde = notation.serde(longType, false)) {
            softly.assertThat(serde)
                    .as("Long type should have serde")
                    .isNotNull();
        }

        // String type should use BinaryNotation.BinarySerde wrapper
        final var stringType = new SimpleType(String.class, "string");
        try (var serde = notation.serde(stringType, false)) {
            softly.assertThat(serde)
                    .as("String type should have serde")
                    .isNotNull();
        }

        softly.assertAll();
    }

    @Test
    @DisplayName("Throws exception for complex types without SerdeSupplier")
    @SuppressWarnings("resource")
        // Expected to throw, no serde created to close
    void unsupportedComplexTypeWithoutSupplierThrowsException() {
        // Given: Binary notation without a complex type serde supplier
        final var notation = new BinaryNotation();

        // When/Then: trying to create serde for a complex type should throw
        final var complexType = new ListType();

        assertThatThrownBy(() -> notation.serde(complexType, false))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("serde");
    }

    @Test
    @DisplayName("Delegates complex types to SerdeSupplier when provided")
    void complexTypeWithSupplier() {
        // Given: Binary notation with a mock SerdeSupplier
        final var mockSupplier = (SerdeSupplier) (type, isKey) -> new Serde<>() {
            @Override
            public Serializer<Object> serializer() {
                return (topic, data) -> new byte[0];
            }

            @Override
            public Deserializer<Object> deserializer() {
                return (topic, data) -> new Object();
            }
        };
        final var notation = new BinaryNotation(mockSupplier);

        // When: requesting serde for a complex type
        final var complexType = new ListType();
        try (var serde = notation.serde(complexType, false)) {
            // Then: should return serde from the supplier
            assertThat(serde).isNotNull();
        }
    }

    @Test
    @DisplayName("File extension is null (binary data doesn't have a standard extension in KSML)")
    void filenameExtension() {
        // Given: Binary notation
        final var notation = new BinaryNotation();

        // Then: extension should be null (not defined for binary notation)
        assertThat(notation.filenameExtension()).isNull();
    }

    @Test
    @DisplayName("DEFAULT_TYPE is UNKNOWN")
    void defaultTypeIsUnknown() {
        // Given: Binary notation's DEFAULT_TYPE
        final var defaultType = BinaryNotation.DEFAULT_TYPE;

        // Then: should be DataType.UNKNOWN
        assertThat(defaultType).isEqualTo(DataType.UNKNOWN);
    }

    @Test
    @DisplayName("Serde works for both key and value serialization")
    void keyAndValueSerde() {
        // Given: Binary notation
        final var notation = new BinaryNotation();
        final var byteType = new SimpleType(Byte.class, "byte");

        // When/Then: create serdes for both key and value
        final var softly = new SoftAssertions();

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
