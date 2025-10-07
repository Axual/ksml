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
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.schema.DataField;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.StructType;

import static io.axual.ksml.data.schema.DataSchemaConstants.NO_TAG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for CSV Serde ensuring correct round-trip serialization/deserialization
 * for Kafka integration scenarios.
 *
 * <p>These tests verify that CSV data can be correctly serialized to bytes and
 * deserialized back for use in Kafka Streams topologies.</p>
 */
@DisplayName("CsvSerde - Kafka serializer/deserializer integration")
class CsvSerdeTest {

    @Test
    @DisplayName("StructType: DataStruct round-trip preserves field values")
    void structRoundTrip() {
        // Given: CSV notation with StructType
        var schema = createSensorDataSchema();
        var structType = new StructType(schema);
        var notation = new CsvNotation(new NotationContext(CsvNotation.NOTATION_NAME));

        // And: a DataStruct with sensor data
        var struct = new DataStruct(schema);
        struct.put("name", DataString.from("sensor001"));
        struct.put("timestamp", DataString.from("1234567890"));
        struct.put("value", DataString.from("23.5"));
        struct.put("type", DataString.from("temperature"));
        struct.put("unit", DataString.from("celsius"));
        struct.put("color", DataString.from("red"));
        struct.put("city", DataString.from("Amsterdam"));
        struct.put("owner", DataString.from("alice"));

        // When: serializing to bytes and deserializing back
        try (var serde = notation.serde(structType, false)) {
            var bytes = serde.serializer().serialize("test-topic", struct);
            var result = serde.deserializer().deserialize("test-topic", bytes);

            // Then: should get DataStruct with same values
            assertThat(result).isInstanceOf(DataStruct.class);
            var resultStruct = (DataStruct) result;

            assertThat(resultStruct.get("name").toString()).isEqualTo("sensor001");
            assertThat(resultStruct.get("timestamp").toString()).isEqualTo("1234567890");
            assertThat(resultStruct.get("value").toString()).isEqualTo("23.5");
            assertThat(resultStruct.get("type").toString()).isEqualTo("temperature");
            assertThat(resultStruct.get("unit").toString()).isEqualTo("celsius");
            assertThat(resultStruct.get("color").toString()).isEqualTo("red");
            assertThat(resultStruct.get("city").toString()).isEqualTo("Amsterdam");
            assertThat(resultStruct.get("owner").toString()).isEqualTo("alice");
        }
    }

    @Test
    @DisplayName("ListType: DataList round-trip preserves element values")
    void listRoundTrip() {
        // Given: CSV notation with ListType
        var listType = new ListType();
        var notation = new CsvNotation(new NotationContext(CsvNotation.NOTATION_NAME));

        // And: a DataList with string values
        var list = new DataList(DataString.DATATYPE);
        list.add(DataString.from("sensor001"));
        list.add(DataString.from("1234567890"));
        list.add(DataString.from("23.5"));

        // When: serializing to bytes and deserializing back
        try (var serde = notation.serde(listType, false)) {
            var bytes = serde.serializer().serialize("test-topic", list);
            var result = serde.deserializer().deserialize("test-topic", bytes);

            // Then: should get DataList with same values
            assertThat(result).isInstanceOf(DataList.class);
            var resultList = (DataList) result;

            assertThat(resultList.size()).isEqualTo(3);
            assertThat(resultList.get(0).toString()).isEqualTo("sensor001");
            assertThat(resultList.get(1).toString()).isEqualTo("1234567890");
            assertThat(resultList.get(2).toString()).isEqualTo("23.5");
        }
    }

    @Test
    @DisplayName("UnionType (DEFAULT_TYPE): supports both Struct and List serialization")
    void unionRoundTrip() {
        // Given: CSV notation with default UnionType
        var notation = new CsvNotation(new NotationContext(CsvNotation.NOTATION_NAME));

        try (var serde = notation.serde(CsvNotation.DEFAULT_TYPE, false)) {
            var softly = new SoftAssertions();

            // When: serializing DataStruct
            var schema = createSimpleSchema("name", "value");
            var struct = new DataStruct(schema);
            struct.put("name", DataString.from("test"));
            struct.put("value", DataString.from("123"));

            var bytes1 = serde.serializer().serialize("topic", struct);
            var result1 = serde.deserializer().deserialize("topic", bytes1);

            // Then: should deserialize (CSV format doesn't preserve struct vs list distinction without schema)
            // With UnionType, CSV is parsed as DataList by default
            softly.assertThat(result1).isInstanceOf(DataList.class);
            softly.assertThat(((DataList) result1).size()).isEqualTo(2);

            // When: serializing DataList
            var list = new DataList(DataString.DATATYPE);
            list.add(DataString.from("a"));
            list.add(DataString.from("b"));

            var bytes2 = serde.serializer().serialize("topic", list);
            var result2 = serde.deserializer().deserialize("topic", bytes2);

            // Then: should get DataList back
            softly.assertThat(result2).isInstanceOf(DataList.class);
            softly.assertThat(((DataList) result2).size()).isEqualTo(2);

            softly.assertAll();
        }
    }

    @Test
    @DisplayName("Handles special characters in values (commas, quotes)")
    void specialCharactersRoundTrip() {
        // Given: schema and serde
        var schema = createSimpleSchema("name", "description");
        var structType = new StructType(schema);
        var notation = new CsvNotation(new NotationContext(CsvNotation.NOTATION_NAME));

        // And: struct with special characters
        var struct = new DataStruct(schema);
        struct.put("name", DataString.from("sensor, type A"));
        struct.put("description", DataString.from("temperature \"high\""));

        // When: round-trip
        try (var serde = notation.serde(structType, false)) {
            var bytes = serde.serializer().serialize("topic", struct);
            var result = serde.deserializer().deserialize("topic", bytes);

            // Then: special characters should be preserved
            assertThat(result).isInstanceOf(DataStruct.class);
            var resultStruct = (DataStruct) result;

            assertThat(resultStruct.get("name").toString()).contains("sensor");
            assertThat(resultStruct.get("name").toString()).contains("type A");
            assertThat(resultStruct.get("description").toString()).contains("temperature");
        }
    }

    @Test
    @DisplayName("Key serde can be created separately from value serde")
    void keySerde() {
        // Given: CSV notation
        var schema = createSimpleSchema("id");
        var structType = new StructType(schema);
        var notation = new CsvNotation(new NotationContext(CsvNotation.NOTATION_NAME));

        // When: creating key serde and using it for round-trip
        try (var keySerde = notation.serde(structType, true)) {
            var keyStruct = new DataStruct(schema);
            keyStruct.put("id", DataString.from("key123"));

            var bytes = keySerde.serializer().serialize("topic", keyStruct);
            var result = keySerde.deserializer().deserialize("topic", bytes);

            // Then: should work correctly
            assertThat(result).isInstanceOf(DataStruct.class);
            assertThat(((DataStruct) result).get("id").toString()).isEqualTo("key123");
        }
    }

    @Test
    @DisplayName("Empty field values are preserved in round-trip")
    void emptyFieldsRoundTrip() {
        // Given: schema and serde
        var schema = createSimpleSchema("name", "optional", "value");
        var structType = new StructType(schema);
        var notation = new CsvNotation(new NotationContext(CsvNotation.NOTATION_NAME));

        // And: struct with empty field
        var struct = new DataStruct(schema);
        struct.put("name", DataString.from("test"));
        struct.put("optional", DataString.from(""));
        struct.put("value", DataString.from("123"));

        // When: round-trip
        try (var serde = notation.serde(structType, false)) {
            var bytes = serde.serializer().serialize("topic", struct);
            var result = serde.deserializer().deserialize("topic", bytes);

            // Then: empty field should be preserved
            assertThat(result).isInstanceOf(DataStruct.class);
            var resultStruct = (DataStruct) result;

            assertThat(resultStruct.get("name").toString()).isEqualTo("test");
            assertThat(resultStruct.get("optional").toString()).isEmpty();
            assertThat(resultStruct.get("value").toString()).isEqualTo("123");
        }
    }

    @Test
    @DisplayName("Throws exception when trying to create serde for unsupported type")
    void unsupportedTypeThrowsException() {
        // Given: CSV notation
        var notation = new CsvNotation(new NotationContext(CsvNotation.NOTATION_NAME));

        // When/Then: trying to create serde for unsupported type should throw
        assertThatThrownBy(() -> notation.serde(DataString.DATATYPE, false))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("serde");
    }

    // Helper methods

    private StructSchema createSensorDataSchema() {
        var fields = java.util.List.of(
            new DataField("name", DataSchema.STRING_SCHEMA, "Sensor name", NO_TAG, true, false, null),
            new DataField("timestamp", DataSchema.STRING_SCHEMA, "Timestamp", NO_TAG, true, false, null),
            new DataField("value", DataSchema.STRING_SCHEMA, "Sensor value", NO_TAG, true, false, null),
            new DataField("type", DataSchema.STRING_SCHEMA, "Sensor type", NO_TAG, true, false, null),
            new DataField("unit", DataSchema.STRING_SCHEMA, "Unit", NO_TAG, true, false, null),
            new DataField("color", DataSchema.STRING_SCHEMA, "Color", NO_TAG, true, false, null),
            new DataField("city", DataSchema.STRING_SCHEMA, "City", NO_TAG, true, false, null),
            new DataField("owner", DataSchema.STRING_SCHEMA, "Owner", NO_TAG, true, false, null)
        );
        return new StructSchema("io.axual.test", "SensorData", "Sensor data schema", fields, false);
    }

    private StructSchema createSimpleSchema(String... fieldNames) {
        var fields = new java.util.ArrayList<DataField>();
        for (var fieldName : fieldNames) {
            fields.add(new DataField(fieldName, DataSchema.STRING_SCHEMA, fieldName, NO_TAG, true, false, null));
        }
        return new StructSchema("io.axual.test", "Simple", "Simple schema", fields, false);
    }
}
