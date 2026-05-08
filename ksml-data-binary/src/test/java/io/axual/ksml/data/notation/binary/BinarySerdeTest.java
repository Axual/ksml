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

import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.type.SimpleType;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for Binary Serde ensuring correct round-trip serialization/deserialization
 * for Kafka integration scenarios with binary data.
 *
 * <p>These tests verify that binary data (raw bytes and primitive types) can be correctly
 * serialized to bytes and deserialized back for use in Kafka Streams topologies.</p>
 */
@DisplayName("BinarySerde - Kafka serializer/deserializer integration")
class BinarySerdeTest {

    @Test
    @DisplayName("Byte round-trip preserves value")
    void byteRoundTrip() {
        // Given: Binary notation with Byte type
        final var byteType = new SimpleType(Byte.class, "byte");
        final var notation = new BinaryNotation(new NotationContext(), null);

        // And: a byte value
        final Byte value = (byte) 42;

        // When: serializing to bytes and deserializing back
        try (var serde = notation.serde(byteType, false)) {
            final var bytes = serde.serializer().serialize("test-topic", value);
            final var result = serde.deserializer().deserialize("test-topic", bytes);

            // Then: should get the same byte value back
            assertThat(result).isInstanceOf(Byte.class);
            assertThat(result).isEqualTo(value);
        }
    }

    @Test
    @DisplayName("Integer round-trip preserves value")
    void integerRoundTrip() {
        // Given: Binary notation with Integer type
        final var intType = new SimpleType(Integer.class, "int");
        final var notation = new BinaryNotation(null);

        // And: an integer value
        final Integer value = 123456;

        // When: serializing to bytes and deserializing back
        try (var serde = notation.serde(intType, false)) {
            final var bytes = serde.serializer().serialize("test-topic", value);
            final var result = serde.deserializer().deserialize("test-topic", bytes);

            // Then: should get the same integer value back
            assertThat(result).isInstanceOf(Integer.class);
            assertThat(result).isEqualTo(value);
        }
    }

    @Test
    @DisplayName("Long round-trip preserves value")
    void longRoundTrip() {
        // Given: Binary notation with Long type
        final var longType = new SimpleType(Long.class, "long");
        final var notation = new BinaryNotation();

        // And: a long value
        final Long value = 9876543210L;

        // When: serializing to bytes and deserializing back
        try (var serde = notation.serde(longType, false)) {
            final var bytes = serde.serializer().serialize("test-topic", value);
            final var result = serde.deserializer().deserialize("test-topic", bytes);

            // Then: should get the same long value back
            assertThat(result).isInstanceOf(Long.class);
            assertThat(result).isEqualTo(value);
        }
    }

    @Test
    @DisplayName("String round-trip preserves value")
    void stringRoundTrip() {
        // Given: Binary notation with String type
        final var stringType = new SimpleType(String.class, "string");
        final var notation = new BinaryNotation();

        // And: a string value
        final String value = "Hello KSML";

        // When: serializing to bytes and deserializing back
        try (var serde = notation.serde(stringType, false)) {
            final var bytes = serde.serializer().serialize("test-topic", value);
            final var result = serde.deserializer().deserialize("test-topic", bytes);

            // Then: should get the same string value back
            assertThat(result).isInstanceOf(String.class);
            assertThat(result).isEqualTo(value);
        }
    }

    @ParameterizedTest
    @DisplayName("Multiple byte values round-trip correctly")
    @ValueSource(bytes = {0, 1, 42, 127, -1, -128, (byte) 255})
    void multipleByteRoundTrips(byte value) {
        // Given: Binary notation with a Byte type
        final var byteType = new SimpleType(Byte.class, "byte");
        final var notation = new BinaryNotation();

        // When: serializing to bytes and deserializing back
        try (var serde = notation.serde(byteType, false)) {
            final var bytes = serde.serializer().serialize("topic", value);
            final var result = serde.deserializer().deserialize("topic", bytes);

            // Then: should round-trip correctly
            assertThat(result)
                    .as("Byte value %d should round-trip correctly", value)
                    .isEqualTo(value);
        }
    }

    @Test
    @DisplayName("Binary data similar to docs example round-trips correctly")
    void binaryMessageLikeDocsExample() {
        // Given: Binary notation with an Integer type (simulating byte array operations)
        final var intType = new SimpleType(Integer.class, "int");
        final var notation = new BinaryNotation();

        // And: values simulating the binary producer example (counter byte)
        final var softly = new SoftAssertions();

        // Test counter values like in the docs example
        try (var serde = notation.serde(intType, false)) {
            for (int counter = 0; counter < 10; counter++) {
                int counterByte = counter % 256;

                // When: serializing and deserializing
                var bytes = serde.serializer().serialize("ksml_sensordata_binary", counterByte);
                var result = serde.deserializer().deserialize("ksml_sensordata_binary", bytes);

                // Then: value should be preserved
                softly.assertThat(result)
                        .as("Counter byte %d should round-trip correctly", counterByte)
                        .isEqualTo(counterByte);
            }
        }

        softly.assertAll();
    }

    @Test
    @DisplayName("Key serde works independently from value serde")
    void keySerde() {
        // Given: Binary notation
        final var stringType = new SimpleType(String.class, "string");
        final var notation = new BinaryNotation();

        // When: creating a key serde and using it for a round-trip
        try (var keySerde = notation.serde(stringType, true)) {
            final String keyValue = "key123";

            final var bytes = keySerde.serializer().serialize("topic", keyValue);
            final var result = keySerde.deserializer().deserialize("topic", bytes);

            // Then: should work correctly
            assertThat(result).isInstanceOf(String.class);
            assertThat(result).isEqualTo(keyValue);
        }
    }

    @Test
    @DisplayName("Null serialization produces null bytes and deserializes to null")
    void nullRoundTrip() {
        // Given: Binary notation with a String type
        final var stringType = new SimpleType(String.class, "string");
        final var notation = new BinaryNotation();

        // When: serializing null
        try (var serde = notation.serde(stringType, false)) {
            final var bytes = serde.serializer().serialize("topic", null);

            // Then: bytes should be null
            assertThat(bytes).isNull();

            // And: deserializing null bytes should return null
            final var result = serde.deserializer().deserialize("topic", null);
            assertThat(result).isNull();
        }
    }

    @Test
    @DisplayName("Empty string round-trips correctly")
    void emptyStringRoundTrip() {
        // Given: Binary notation with a String type
        final var stringType = new SimpleType(String.class, "string");
        final var notation = new BinaryNotation();

        // And: empty string
        final String value = "";

        // When: serializing to bytes and deserializing back
        try (var serde = notation.serde(stringType, false)) {
            final var bytes = serde.serializer().serialize("topic", value);
            final var result = serde.deserializer().deserialize("topic", bytes);

            // Then: should get empty string back
            assertThat(result).isInstanceOf(String.class);
            assertThat(result).isEqualTo(value);
        }
    }

    @Test
    @DisplayName("Special characters in strings round-trip correctly")
    void specialCharactersRoundTrip() {
        // Given: Binary notation with a String type
        final var stringType = new SimpleType(String.class, "string");
        final var notation = new BinaryNotation();

        // When/Then: verify strings with special characters round-trip correctly
        final var softly = new SoftAssertions();

        String[] testStrings = {
                "Hello, World!",
                "KSML",
                "Temperature: 23.5°C",
                "Multi\nLine\nString",
                "Unicode: ❤ ★",
                "Emoji: 🤖"
        };

        try (var serde = notation.serde(stringType, false)) {
            for (String testString : testStrings) {
                final var bytes = serde.serializer().serialize("topic", testString);
                final var result = serde.deserializer().deserialize("topic", bytes);
                softly.assertThat(result)
                        .as("String '%s' should round-trip correctly", testString)
                        .isEqualTo(testString);
            }
        }

        softly.assertAll();
    }

    @Test
    @DisplayName("Integer edge cases round-trip correctly")
    void integerEdgeCases() {
        // Given: Binary notation with an Integer type
        final var intType = new SimpleType(Integer.class, "int");
        final var notation = new BinaryNotation();

        // When/Then: verify edge case integer values round-trip correctly
        try (var serde = notation.serde(intType, false)) {
            final var softly = new SoftAssertions();

            Integer[] testValues = {
                    0,
                    1,
                    -1,
                    Integer.MAX_VALUE,
                    Integer.MIN_VALUE,
                    256,
                    -256
            };

            for (Integer testValue : testValues) {
                final var bytes = serde.serializer().serialize("topic", testValue);
                final var result = serde.deserializer().deserialize("topic", bytes);
                softly.assertThat(result)
                        .as("Integer value %d should round-trip correctly", testValue)
                        .isEqualTo(testValue);
            }

            softly.assertAll();
        }
    }

    @Test
    @DisplayName("Long edge cases round-trip correctly")
    void longEdgeCases() {
        // Given: Binary notation with a Long type
        final var longType = new SimpleType(Long.class, "long");
        final var notation = new BinaryNotation();

        // When/Then: verify edge case long values round-trip correctly
        try (var serde = notation.serde(longType, false)) {
            final var softly = new SoftAssertions();

            Long[] testValues = {
                    0L,
                    1L,
                    -1L,
                    Long.MAX_VALUE,
                    Long.MIN_VALUE,
                    1234567890123456789L,
                    -1234567890123456789L
            };

            for (Long testValue : testValues) {
                final var bytes = serde.serializer().serialize("topic", testValue);
                final var result = serde.deserializer().deserialize("topic", bytes);
                softly.assertThat(result)
                        .as("Long value %d should round-trip correctly", testValue)
                        .isEqualTo(testValue);
            }

            softly.assertAll();
        }
    }

    @Test
    @DisplayName("Binary operations similar to docs processor example")
    void binaryOperationsLikeDocsExample() {
        // Given: Binary notation with a Byte type (simulating byte manipulation)
        final var byteType = new SimpleType(Byte.class, "byte");
        final var notation = new BinaryNotation();

        // Simulate the docs example: increment first byte operation
        final byte originalValue = 42;

        // When: serialize, "process" (increment), serialize again
        try (var serde = notation.serde(byteType, false)) {
            final var bytes = serde.serializer().serialize("topic", originalValue);
            final var deserialized = (Byte) serde.deserializer().deserialize("topic", bytes);

            // Simulate Python: modified[0] = (modified[0] + 1) % 256
            final byte modifiedValue = (byte) ((deserialized + 1) % 256);

            final var modifiedBytes = serde.serializer().serialize("topic", modifiedValue);
            final var finalResult = serde.deserializer().deserialize("topic", modifiedBytes);

            // Then: should have incremented value
            assertThat(finalResult).isEqualTo(modifiedValue);
            assertThat((Byte) finalResult).isEqualTo((byte) 43);
        }
    }
}
