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
        assertThat(DataSchema.BOOLEAN_SCHEMA.isAssignableFrom(null).isAssignable()).isFalse();
        assertThat(DataSchema.BOOLEAN_SCHEMA.isAssignableFrom(DataSchema.BOOLEAN_SCHEMA).isAssignable()).isTrue();
        assertThat(DataSchema.BOOLEAN_SCHEMA.isAssignableFrom(DataSchema.STRING_SCHEMA).isAssignable()).isFalse();
    }

    @Test
    @DisplayName("ANY schema accepts any non-null schema and rejects null")
    void anySchemaAssignability() {
        assertThat(DataSchema.ANY_SCHEMA.isAssignableFrom(null).isAssignable()).isFalse();
        assertThat(DataSchema.ANY_SCHEMA.isAssignableFrom(DataSchema.STRING_SCHEMA).isAssignable()).isTrue();
        assertThat(DataSchema.ANY_SCHEMA.isAssignableFrom(DataSchema.NULL_SCHEMA).isAssignable()).isTrue();
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

    @ParameterizedTest(name = "{index}: {0} accepts only widening integer sources")
    @MethodSource("integerSchemas")
    void integerSchemasFollowWideningOnly(DataSchema target) {
        // byte accepts only byte
        if (target == DataSchema.BYTE_SCHEMA) {
            assertThat(target.isAssignableFrom(DataSchema.BYTE_SCHEMA).isAssignable()).as("byte←byte").isTrue();
            assertThat(target.isAssignableFrom(DataSchema.SHORT_SCHEMA).isAssignable()).as("byte←short (narrowing)").isFalse();
            assertThat(target.isAssignableFrom(DataSchema.INTEGER_SCHEMA).isAssignable()).as("byte←int (narrowing)").isFalse();
            assertThat(target.isAssignableFrom(DataSchema.LONG_SCHEMA).isAssignable()).as("byte←long (narrowing)").isFalse();
        }
        // short accepts byte and short
        if (target == DataSchema.SHORT_SCHEMA) {
            assertThat(target.isAssignableFrom(DataSchema.BYTE_SCHEMA).isAssignable()).as("short←byte").isTrue();
            assertThat(target.isAssignableFrom(DataSchema.SHORT_SCHEMA).isAssignable()).as("short←short").isTrue();
            assertThat(target.isAssignableFrom(DataSchema.INTEGER_SCHEMA).isAssignable()).as("short←int (narrowing)").isFalse();
            assertThat(target.isAssignableFrom(DataSchema.LONG_SCHEMA).isAssignable()).as("short←long (narrowing)").isFalse();
        }
        // int accepts byte, short, int
        if (target == DataSchema.INTEGER_SCHEMA) {
            assertThat(target.isAssignableFrom(DataSchema.BYTE_SCHEMA).isAssignable()).as("int←byte").isTrue();
            assertThat(target.isAssignableFrom(DataSchema.SHORT_SCHEMA).isAssignable()).as("int←short").isTrue();
            assertThat(target.isAssignableFrom(DataSchema.INTEGER_SCHEMA).isAssignable()).as("int←int").isTrue();
            assertThat(target.isAssignableFrom(DataSchema.LONG_SCHEMA).isAssignable()).as("int←long (narrowing)").isFalse();
        }
        // long accepts byte, short, int, long
        if (target == DataSchema.LONG_SCHEMA) {
            assertThat(target.isAssignableFrom(DataSchema.BYTE_SCHEMA).isAssignable()).as("long←byte").isTrue();
            assertThat(target.isAssignableFrom(DataSchema.SHORT_SCHEMA).isAssignable()).as("long←short").isTrue();
            assertThat(target.isAssignableFrom(DataSchema.INTEGER_SCHEMA).isAssignable()).as("long←int").isTrue();
            assertThat(target.isAssignableFrom(DataSchema.LONG_SCHEMA).isAssignable()).as("long←long").isTrue();
        }
        // No integer type accepts floating or string
        assertThat(target.isAssignableFrom(DataSchema.FLOAT_SCHEMA).isAssignable()).isFalse();
        assertThat(target.isAssignableFrom(DataSchema.DOUBLE_SCHEMA).isAssignable()).isFalse();
        assertThat(target.isAssignableFrom(DataSchema.STRING_SCHEMA).isAssignable()).isFalse();
    }

    @ParameterizedTest(name = "{index}: {0} accepts only widening floating-point sources")
    @MethodSource("floatingSchemas")
    void floatingSchemasFollowWideningOnly(DataSchema target) {
        // float accepts only float
        if (target == DataSchema.FLOAT_SCHEMA) {
            assertThat(target.isAssignableFrom(DataSchema.FLOAT_SCHEMA).isAssignable()).as("float←float").isTrue();
            assertThat(target.isAssignableFrom(DataSchema.DOUBLE_SCHEMA).isAssignable()).as("float←double (narrowing)").isFalse();
        }
        // double accepts float and double
        if (target == DataSchema.DOUBLE_SCHEMA) {
            assertThat(target.isAssignableFrom(DataSchema.FLOAT_SCHEMA).isAssignable()).as("double←float").isTrue();
            assertThat(target.isAssignableFrom(DataSchema.DOUBLE_SCHEMA).isAssignable()).as("double←double").isTrue();
        }
        // No floating type accepts integer or string
        assertThat(target.isAssignableFrom(DataSchema.BYTE_SCHEMA).isAssignable()).isFalse();
        assertThat(target.isAssignableFrom(DataSchema.LONG_SCHEMA).isAssignable()).isFalse();
        assertThat(target.isAssignableFrom(DataSchema.STRING_SCHEMA).isAssignable()).isFalse();
    }

    @Test
    @DisplayName("STRING accepts from NULL and ENUM, and itself")
    void stringSpecialAssignability() {
        assertThat(DataSchema.STRING_SCHEMA.isAssignableFrom(DataSchema.NULL_SCHEMA).isAssignable()).isTrue();
        // Create an enum schema instance to test ENUM behavior
        final var enumSchema = new EnumSchema(
                DataSchemaConstants.DATA_SCHEMA_KSML_NAMESPACE,
                "Color",
                "Enum of colors",
                List.of(new EnumSchema.Symbol("RED"), new EnumSchema.Symbol("GREEN"), new EnumSchema.Symbol("BLUE"))
        );
        assertThat(DataSchema.STRING_SCHEMA.isAssignableFrom(enumSchema).isAssignable()).isTrue();
        assertThat(DataSchema.STRING_SCHEMA.isAssignableFrom(DataSchema.STRING_SCHEMA).isAssignable()).isTrue();
        assertThat(DataSchema.STRING_SCHEMA.isAssignableFrom(DataSchema.BOOLEAN_SCHEMA).isAssignable()).isFalse();
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

            assertThat(new DataSchema(DataSchemaConstants.BOOLEAN_TYPE) {
            })
                    .isEqualTo(DataSchema.BOOLEAN_SCHEMA)
                    .isNotEqualTo(DataSchema.STRING_SCHEMA);
        }
    }
}
