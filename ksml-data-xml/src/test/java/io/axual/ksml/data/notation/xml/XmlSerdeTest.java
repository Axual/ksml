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

import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.schema.DataField;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.StructType;

import static io.axual.ksml.data.schema.DataSchemaConstants.NO_TAG;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for XML Serde ensuring correct round-trip serialization/deserialization
 * for Kafka integration scenarios with XML data and XSD schemas.
 *
 * <p>These tests verify that XML data matching the tutorial examples can be correctly
 * serialized to bytes and deserialized back for use in Kafka Streams topologies.</p>
 */
@DisplayName("XmlSerde - Kafka serializer/deserializer integration")
class XmlSerdeTest {

    @Test
    @DisplayName("StructType: SensorData round-trip preserves all field values")
    void sensorDataRoundTrip() {
        // Given: XML notation with SensorData schema (matching tutorial)
        var schema = createSensorDataSchema();
        var structType = new StructType(schema);
        var notation = new XmlNotation(new NotationContext(XmlNotation.NOTATION_NAME));

        // And: a SensorData struct matching the tutorial example
        var struct = new DataStruct(schema);
        struct.put("name", DataString.from("sensor001"));
        struct.put("timestamp", new DataLong(1234567890L));
        struct.put("value", DataString.from("23.5"));
        struct.put("type", DataString.from("TEMPERATURE"));
        struct.put("unit", DataString.from("C"));
        struct.put("color", DataString.from("red"));
        struct.put("city", DataString.from("Amsterdam"));
        struct.put("owner", DataString.from("Alice"));

        // When: serializing to bytes and deserializing back
        try (var serde = notation.serde(structType, false)) {
            var bytes = serde.serializer().serialize("ksml_sensordata_xml", struct);
            var result = serde.deserializer().deserialize("ksml_sensordata_xml", bytes);

            // Then: should get DataStruct with all values preserved
            assertThat(result).isInstanceOf(DataStruct.class);
            var resultStruct = (DataStruct) result;

            assertThat(resultStruct.get("name").toString()).isEqualTo("sensor001");
            assertThat(resultStruct.get("timestamp").toString()).isEqualTo("1234567890");
            assertThat(resultStruct.get("value").toString()).isEqualTo("23.5");
            assertThat(resultStruct.get("type").toString()).isEqualTo("TEMPERATURE");
            assertThat(resultStruct.get("unit").toString()).isEqualTo("C");
            assertThat(resultStruct.get("color").toString()).isEqualTo("red");
            assertThat(resultStruct.get("city").toString()).isEqualTo("Amsterdam");
            assertThat(resultStruct.get("owner").toString()).isEqualTo("Alice");
        }
    }

    @Test
    @DisplayName("Round-trip with uppercase transformation (like tutorial processor)")
    void uppercaseCityTransformationRoundTrip() {
        // Given: XML notation with SensorData schema
        var schema = createSensorDataSchema();
        var structType = new StructType(schema);
        var notation = new XmlNotation(new NotationContext(XmlNotation.NOTATION_NAME));

        // And: sensor data with lowercase city
        var struct = new DataStruct(schema);
        struct.put("name", DataString.from("sensor6"));
        struct.put("timestamp", new DataLong(1754376106863L));
        struct.put("value", DataString.from("12"));
        struct.put("type", DataString.from("TEMPERATURE"));
        struct.put("unit", DataString.from("%"));
        struct.put("color", DataString.from("black"));
        struct.put("city", DataString.from("Rotterdam"));
        struct.put("owner", DataString.from("Alice"));

        // When: round-trip through serde
        try (var serde = notation.serde(structType, false)) {
            var bytes = serde.serializer().serialize("topic", struct);
            var deserialized = (DataStruct) serde.deserializer().deserialize("topic", bytes);

            // Simulate the tutorial transformation: uppercase city
            var transformed = new DataStruct(schema);
            transformed.put("name", deserialized.get("name"));
            transformed.put("timestamp", deserialized.get("timestamp"));
            transformed.put("value", deserialized.get("value"));
            transformed.put("type", deserialized.get("type"));
            transformed.put("unit", deserialized.get("unit"));
            transformed.put("color", deserialized.get("color"));
            transformed.put("city", DataString.from(deserialized.get("city").toString().toUpperCase()));
            transformed.put("owner", deserialized.get("owner"));

            // Then: serialize transformed data
            var transformedBytes = serde.serializer().serialize("topic", transformed);
            var result = (DataStruct) serde.deserializer().deserialize("topic", transformedBytes);

            // And: verify transformation persisted
            assertThat(result.get("city").toString()).isEqualTo("ROTTERDAM");
            assertThat(result.get("name").toString()).isEqualTo("sensor6");
        }
    }

    @Test
    @DisplayName("Special XML characters are preserved through round-trip")
    void specialCharactersRoundTrip() {
        // Given: schema and serde
        var schema = createSimpleSchema("name", "description");
        var structType = new StructType(schema);
        var notation = new XmlNotation(new NotationContext(XmlNotation.NOTATION_NAME));

        // And: struct with special XML characters
        var struct = new DataStruct(schema);
        struct.put("name", DataString.from("test & example"));
        struct.put("description", DataString.from("<value> with \"quotes\" and 'apostrophes'"));

        // When: round-trip
        try (var serde = notation.serde(structType, false)) {
            var bytes = serde.serializer().serialize("topic", struct);
            var result = serde.deserializer().deserialize("topic", bytes);

            // Then: special characters should be preserved
            assertThat(result).isInstanceOf(DataStruct.class);
            var resultStruct = (DataStruct) result;

            assertThat(resultStruct.get("name").toString()).isEqualTo("test & example");
            assertThat(resultStruct.get("description").toString()).contains("<value>");
            assertThat(resultStruct.get("description").toString()).contains("\"quotes\"");
            assertThat(resultStruct.get("description").toString()).contains("'apostrophes'");
        }
    }

    @Test
    @DisplayName("Empty element values are preserved in round-trip")
    void emptyElementsRoundTrip() {
        // Given: schema and serde
        var schema = createSimpleSchema("name", "optional", "value");
        var structType = new StructType(schema);
        var notation = new XmlNotation(new NotationContext(XmlNotation.NOTATION_NAME));

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
    @DisplayName("Key serde works independently from value serde")
    void keySerde() {
        // Given: XML notation
        var schema = createSimpleSchema("id");
        var structType = new StructType(schema);
        var notation = new XmlNotation(new NotationContext(XmlNotation.NOTATION_NAME));

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
    @DisplayName("Null serialization produces null bytes and deserializes to DataStruct with isNull=true")
    void nullRoundTrip() {
        // Given: XML notation with StructType
        var structType = new StructType();
        var notation = new XmlNotation(new NotationContext(XmlNotation.NOTATION_NAME));

        // When: serializing null
        try (var serde = notation.serde(structType, false)) {
            var bytes = serde.serializer().serialize("topic", null);

            // Then: bytes should be null
            assertThat(bytes).isNull();

            // And: deserializing null bytes should return DataStruct with isNull=true
            var result = serde.deserializer().deserialize("topic", null);
            assertThat(result).isInstanceOf(DataStruct.class);
            assertThat(((DataStruct) result).isNull()).isTrue();
        }
    }

    @Test
    @DisplayName("Numeric timestamp field preserves long values through round-trip")
    void numericTypeRoundTrip() {
        // Given: schema with long timestamp (like tutorial SensorData)
        var fields = java.util.List.of(
                new DataField("name", DataSchema.STRING_SCHEMA, "Name", NO_TAG, true, false, null),
                new DataField("timestamp", DataSchema.LONG_SCHEMA, "Timestamp", NO_TAG, true, false, null),
                new DataField("value", DataSchema.STRING_SCHEMA, "Value", NO_TAG, true, false, null)
        );
        var schema = new StructSchema("io.axual.test", "TimestampData", "Data with timestamp", fields, false);
        var structType = new StructType(schema);
        var notation = new XmlNotation(new NotationContext(XmlNotation.NOTATION_NAME));

        // And: struct with long timestamp
        var struct = new DataStruct(schema);
        struct.put("name", DataString.from("sensor1"));
        struct.put("timestamp", new DataLong(1754376106863L));
        struct.put("value", DataString.from("42"));

        // When: round-trip
        try (var serde = notation.serde(structType, false)) {
            var bytes = serde.serializer().serialize("topic", struct);
            var result = serde.deserializer().deserialize("topic", bytes);

            // Then: numeric value should be preserved
            assertThat(result).isInstanceOf(DataStruct.class);
            var resultStruct = (DataStruct) result;
            assertThat(resultStruct.get("timestamp").toString()).isEqualTo("1754376106863");
        }
    }

    @Test
    @DisplayName("Multiple different city names round-trip correctly (like tutorial data)")
    void multipleCitiesRoundTrip() {
        // Given: XML notation with simple schema
        var schema = createSimpleSchema("city");
        var structType = new StructType(schema);
        var notation = new XmlNotation(new NotationContext(XmlNotation.NOTATION_NAME));

        // When/Then: verify different city names from tutorial round-trip correctly
        try (var serde = notation.serde(structType, false)) {
            var softly = new SoftAssertions();

            String[] cities = {"Amsterdam", "Utrecht", "Rotterdam", "The Hague", "Eindhoven"};
            for (String city : cities) {
                var struct = new DataStruct(schema);
                struct.put("city", DataString.from(city));

                var bytes = serde.serializer().serialize("topic", struct);
                var result = serde.deserializer().deserialize("topic", bytes);

                softly.assertThat(result).isInstanceOf(DataStruct.class);
                softly.assertThat(((DataStruct) result).get("city").toString())
                        .as("City '%s' should round-trip correctly", city)
                        .isEqualTo(city);
            }

            softly.assertAll();
        }
    }

    @Test
    @DisplayName("All sensor types from tutorial round-trip correctly")
    void sensorTypesRoundTrip() {
        // Given: XML notation
        var schema = createSimpleSchema("type", "unit");
        var structType = new StructType(schema);
        var notation = new XmlNotation(new NotationContext(XmlNotation.NOTATION_NAME));

        // When/Then: verify sensor types and units from tutorial
        try (var serde = notation.serde(structType, false)) {
            var softly = new SoftAssertions();

            Object[][] typesAndUnits = {
                    {"TEMPERATURE", "C"},
                    {"TEMPERATURE", "F"},
                    {"HUMIDITY", "%"},
                    {"PRESSURE", "Pa"}
            };

            for (Object[] typeAndUnit : typesAndUnits) {
                var struct = new DataStruct(schema);
                struct.put("type", DataString.from((String) typeAndUnit[0]));
                struct.put("unit", DataString.from((String) typeAndUnit[1]));

                var bytes = serde.serializer().serialize("topic", struct);
                var result = serde.deserializer().deserialize("topic", bytes);

                softly.assertThat(result).isInstanceOf(DataStruct.class);
                var resultStruct = (DataStruct) result;
                softly.assertThat(resultStruct.get("type").toString()).isEqualTo(typeAndUnit[0]);
                softly.assertThat(resultStruct.get("unit").toString()).isEqualTo(typeAndUnit[1]);
            }

            softly.assertAll();
        }
    }

    @Test
    @DisplayName("Optional fields (minOccurs=0 in XSD) can be omitted")
    void optionalFieldsHandling() {
        // Given: schema with required and optional fields (like tutorial SensorData)
        var schema = createSchemaWithOptionalFields();
        var structType = new StructType(schema);
        var notation = new XmlNotation(new NotationContext(XmlNotation.NOTATION_NAME));

        // And: struct with only required fields
        var struct = new DataStruct(schema);
        struct.put("name", DataString.from("sensor1"));
        struct.put("value", DataString.from("42"));
        // Optional fields not set

        // When: round-trip
        try (var serde = notation.serde(structType, false)) {
            var bytes = serde.serializer().serialize("topic", struct);
            var result = serde.deserializer().deserialize("topic", bytes);

            // Then: should work without optional fields
            assertThat(result).isInstanceOf(DataStruct.class);
            var resultStruct = (DataStruct) result;
            assertThat(resultStruct.get("name").toString()).isEqualTo("sensor1");
            assertThat(resultStruct.get("value").toString()).isEqualTo("42");
        }
    }

    // Helper methods

    private StructSchema createSensorDataSchema() {
        // Schema matching the tutorial SensorData.xsd
        var fields = java.util.List.of(
                new DataField("name", DataSchema.STRING_SCHEMA, "Sensor name", NO_TAG, true, false, null),
                new DataField("timestamp", DataSchema.LONG_SCHEMA, "Timestamp", NO_TAG, true, false, null),
                new DataField("value", DataSchema.STRING_SCHEMA, "Sensor value", NO_TAG, true, false, null),
                new DataField("type", DataSchema.STRING_SCHEMA, "Sensor type", NO_TAG, true, false, null),
                new DataField("unit", DataSchema.STRING_SCHEMA, "Unit", NO_TAG, true, false, null),
                new DataField("color", DataSchema.STRING_SCHEMA, "Color", NO_TAG, false, false, null),
                new DataField("city", DataSchema.STRING_SCHEMA, "City", NO_TAG, false, false, null),
                new DataField("owner", DataSchema.STRING_SCHEMA, "Owner", NO_TAG, false, false, null)
        );
        return new StructSchema("io.axual.test", "SensorData", "Sensor data schema", fields, false);
    }

    private StructSchema createSimpleSchema(String... fieldNames) {
        var fields = new java.util.ArrayList<DataField>();
        for (var fieldName : fieldNames) {
            fields.add(new DataField(fieldName, DataSchema.STRING_SCHEMA, fieldName, NO_TAG, true, false, null));
        }
        return new StructSchema("io.axual.test", "SimpleData", "Simple schema", fields, false);
    }

    private StructSchema createSchemaWithOptionalFields() {
        var fields = java.util.List.of(
                new DataField("name", DataSchema.STRING_SCHEMA, "Name", NO_TAG, true, false, null),
                new DataField("value", DataSchema.STRING_SCHEMA, "Value", NO_TAG, true, false, null),
                // Optional fields like in SensorData.xsd
                new DataField("color", DataSchema.STRING_SCHEMA, "Color", NO_TAG, false, false, null),
                new DataField("city", DataSchema.STRING_SCHEMA, "City", NO_TAG, false, false, null)
        );
        return new StructSchema("io.axual.test", "OptionalData", "Data with optional fields", fields, false);
    }
}
