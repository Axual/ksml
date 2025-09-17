package io.axual.ksml.data.schema;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
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

import io.axual.ksml.data.type.Symbol;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class DataSchemaTest {

    @Test
    @DisplayName("toString returns the schema type")
    void toStringReturnsType() {
        assertThat(DataSchema.STRING_SCHEMA.toString()).hasToString(DataSchemaConstants.STRING_TYPE);
        assertThat(DataSchema.BOOLEAN_SCHEMA.toString()).hasToString(DataSchemaConstants.BOOLEAN_TYPE);
    }

    @Test
    @DisplayName("Base isAssignableFrom matches by type and handles null")
    void baseIsAssignableFrom() {
        assertThat(DataSchema.BOOLEAN_SCHEMA.isAssignableFrom(null)).isFalse();
        assertThat(DataSchema.BOOLEAN_SCHEMA.isAssignableFrom(DataSchema.BOOLEAN_SCHEMA)).isTrue();
        assertThat(DataSchema.BOOLEAN_SCHEMA.isAssignableFrom(DataSchema.STRING_SCHEMA)).isFalse();
    }

    @Test
    @DisplayName("ANY schema accepts any non-null schema and rejects null")
    void anySchemaAssignability() {
        assertThat(DataSchema.ANY_SCHEMA.isAssignableFrom(null)).isFalse();
        assertThat(DataSchema.ANY_SCHEMA.isAssignableFrom(DataSchema.STRING_SCHEMA)).isTrue();
        assertThat(DataSchema.ANY_SCHEMA.isAssignableFrom(DataSchema.NULL_SCHEMA)).isTrue();
    }

    static Stream<DataSchema> integerSchemas() {
        return Stream.of(
                DataSchema.BYTE_SCHEMA,
                DataSchema.SHORT_SCHEMA,
                DataSchema.INTEGER_SCHEMA,
                DataSchema.LONG_SCHEMA
        );
    }

    static Stream<DataSchema> floatingSchemas() {
        return Stream.of(
                DataSchema.FLOAT_SCHEMA,
                DataSchema.DOUBLE_SCHEMA
        );
    }

    @ParameterizedTest(name = "{index}: {0} accepts all integer types")
    @MethodSource("integerSchemas")
    void integerSchemasAreAssignableFromAllIntegerTypes(DataSchema target) {
        for (var candidate : new DataSchema[]{
                DataSchema.BYTE_SCHEMA,
                DataSchema.SHORT_SCHEMA,
                DataSchema.INTEGER_SCHEMA,
                DataSchema.LONG_SCHEMA
        }) {
            assertThat(target.isAssignableFrom(candidate))
                    .as(target + " should accept from " + candidate)
                    .isTrue();
        }
        // But should not accept from floating or string
        assertThat(target.isAssignableFrom(DataSchema.FLOAT_SCHEMA)).isFalse();
        assertThat(target.isAssignableFrom(DataSchema.DOUBLE_SCHEMA)).isFalse();
        assertThat(target.isAssignableFrom(DataSchema.STRING_SCHEMA)).isFalse();
    }

    @ParameterizedTest(name = "{index}: {0} accepts all floating-point types")
    @MethodSource("floatingSchemas")
    void floatingSchemasAreAssignableFromAllFloatingTypes(DataSchema target) {
        for (var candidate : new DataSchema[]{
                DataSchema.FLOAT_SCHEMA,
                DataSchema.DOUBLE_SCHEMA
        }) {
            assertThat(target.isAssignableFrom(candidate))
                    .as(target + " should accept from " + candidate)
                    .isTrue();
        }
        // But should not accept from integer or string
        assertThat(target.isAssignableFrom(DataSchema.BYTE_SCHEMA)).isFalse();
        assertThat(target.isAssignableFrom(DataSchema.LONG_SCHEMA)).isFalse();
        assertThat(target.isAssignableFrom(DataSchema.STRING_SCHEMA)).isFalse();
    }

    @Test
    @DisplayName("STRING accepts from NULL and ENUM, and itself")
    void stringSpecialAssignability() {
        assertThat(DataSchema.STRING_SCHEMA.isAssignableFrom(DataSchema.NULL_SCHEMA)).isTrue();
        // Create an enum schema instance to test ENUM behavior
        final var enumSchema = new EnumSchema(
                DataSchemaConstants.DATA_SCHEMA_KSML_NAMESPACE,
                "Color",
                "Enum of colors",
                List.of(new Symbol("RED"), new Symbol("GREEN"), new Symbol("BLUE"))
        );
        assertThat(DataSchema.STRING_SCHEMA.isAssignableFrom(enumSchema)).isTrue();
        assertThat(DataSchema.STRING_SCHEMA.isAssignableFrom(DataSchema.STRING_SCHEMA)).isTrue();
        assertThat(DataSchema.STRING_SCHEMA.isAssignableFrom(DataSchema.BOOLEAN_SCHEMA)).isFalse();
    }

    @Nested
    class EqualityAndHashCode {
        @Test
        void equalsAndHashCodeBasedOnType() {
            // Using protected constructor from same package to create another instance with same type
            final var customString = new DataSchema(DataSchemaConstants.STRING_TYPE) {
            };
            assertThat(customString)
                    .isEqualTo(DataSchema.STRING_SCHEMA)
                    .hasSameHashCodeAs(DataSchema.STRING_SCHEMA);

            assertThat(new DataSchema(DataSchemaConstants.BOOLEAN_TYPE) {})
                    .isEqualTo(DataSchema.BOOLEAN_SCHEMA)
                    .isNotEqualTo(DataSchema.STRING_SCHEMA);
        }
    }
}
