package io.axual.ksml.data.notation.csv;

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
import io.axual.ksml.data.type.SimpleType;
import io.axual.ksml.data.type.StructType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link CsvNotation} ensuring correct defaults, serde selection for supported types,
 * and proper integration with KSML's notation system.
 *
 * <p>These tests verify that users can specify 'csv' as a data format in KSML definitions.</p>
 */
@DisplayName("CsvNotation - defaults, serde selection, and wiring")
class CsvNotationTest {

    @Test
    @DisplayName("Default properties: name, extension, default type, converter, parser")
    void basicProperties() {
        // Given: a standard CSV notation context (no vendor)
        var context = new NotationContext(CsvNotation.NOTATION_NAME);

        // When: constructing CsvNotation
        var notation = new CsvNotation(context);

        // Then: verify defaults using chained assertions
        assertThat(notation)
                .returns("csv", BaseNotation::name)
                .returns(".csv", BaseNotation::filenameExtension)
                .returns(CsvNotation.DEFAULT_TYPE, CsvNotation::defaultType);

        // Converter should be CsvDataObjectConverter
        assertThat(notation.converter()).isInstanceOf(CsvDataObjectConverter.class);

        // Schema parser should be CsvSchemaParser
        assertThat(notation.schemaParser()).isInstanceOf(CsvSchemaParser.class);
    }

    @Test
    @DisplayName("Name includes vendor prefix when provided by context")
    void nameWithVendor() {
        // Given: context with vendor
        var context = new NotationContext(CsvNotation.NOTATION_NAME, "vendorX");

        // When: constructing notation
        var notation = new CsvNotation(context);

        // Then: name should include vendor prefix
        assertThat(notation.name()).isEqualTo("vendorX_csv");
    }

    @Test
    @DisplayName("Serde is provided for ListType, StructType, and DEFAULT_TYPE (Union)")
    void serdeForSupportedTypes() {
        // Given: CSV notation
        var notation = new CsvNotation(new NotationContext(CsvNotation.NOTATION_NAME));

        // When/Then: verify serde creation for supported types
        var softly = new SoftAssertions();

        // ListType is supported
        try (var serde = notation.serde(new ListType(), false)) {
            softly.assertThat(serde)
                    .as("ListType should have serde")
                    .isNotNull();
        }

        // StructType is supported
        try (var serde = notation.serde(new StructType(), false)) {
            softly.assertThat(serde)
                    .as("StructType should have serde")
                    .isNotNull();
        }

        // DEFAULT_TYPE (Union of List and Struct) is supported
        try (var serde = notation.serde(CsvNotation.DEFAULT_TYPE, false)) {
            softly.assertThat(serde)
                    .as("DEFAULT_TYPE should have serde")
                    .isNotNull();
        }

        // Verify for both key and value serdes
        try (var serde = notation.serde(new ListType(), true)) {
            softly.assertThat(serde)
                    .as("ListType key serde should be created")
                    .isNotNull();
        }

        try (var serde = notation.serde(new StructType(), true)) {
            softly.assertThat(serde)
                    .as("StructType key serde should be created")
                    .isNotNull();
        }

        softly.assertAll();
    }

    @Test
    @DisplayName("Throws exception for unsupported type (not List or Struct)")
    @SuppressWarnings("resource") // Expected to throw, no serde created to close
    void unsupportedTypeThrowsException() {
        // Given: CSV notation
        var notation = new CsvNotation(new NotationContext(CsvNotation.NOTATION_NAME));

        // When/Then: trying to create serde for unsupported type should throw
        var unsupportedType = new SimpleType(Integer.class, "int");

        assertThatThrownBy(() -> notation.serde(unsupportedType, false))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("serde");
    }

    @Test
    @DisplayName("File extension is .csv")
    void filenameExtension() {
        // Given: CSV notation
        var notation = new CsvNotation(new NotationContext(CsvNotation.NOTATION_NAME));

        // Then: extension should be .csv
        assertThat(notation.filenameExtension()).isEqualTo(".csv");
    }

    @Test
    @DisplayName("DEFAULT_TYPE is Union of StructType and ListType")
    void defaultTypeIsUnion() {
        // Given: CSV notation's DEFAULT_TYPE
        var defaultType = CsvNotation.DEFAULT_TYPE;

        // Then: should be a UnionType that accepts both Struct and List
        assertThat(defaultType).isNotNull();

        // Verify it accepts StructType
        assertThat(defaultType.isAssignableFrom(new StructType()).isOK()).isTrue();

        // Verify it accepts ListType
        assertThat(defaultType.isAssignableFrom(new ListType()).isOK()).isTrue();
    }
}
