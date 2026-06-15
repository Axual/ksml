package io.axual.ksml.data.notation.json;

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

import io.axual.ksml.data.schema.StructSchema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies KSML schema-evolution compatibility rules for JSON Schema across 18 scenarios.
 * Convention: s2.isAssignableFrom(s1) = backward compat; s1.isAssignableFrom(s2) = forward compat.
 */
class JsonSchemaEvolutionCompatibilityTest {

    private static final String NS = "io.axual.ksml.test";
    private final JsonSchemaMapper mapper = new JsonSchemaMapper(false);

    private StructSchema toStruct(String name, String json) {
        var result = mapper.toDataSchema(NS, name, json);
        assertThat(result).isInstanceOf(StructSchema.class);
        return (StructSchema) result;
    }

    // ===== OPTIONAL FIELDS =====

    @Test
    @DisplayName("Add optional field — backward compatible")
    void addOptionalField_backward_pass() {
        var v1 = toStruct("Event", """
                {
                  "type": "object",
                  "properties": {
                    "name":      { "type": "string" },
                    "timestamp": { "type": "integer" },
                    "value":     { "type": "string" }
                  },
                  "required": ["name", "timestamp", "value"]
                }
                """);
        var v2 = toStruct("Event", """
                {
                  "type": "object",
                  "properties": {
                    "name":      { "type": "string" },
                    "timestamp": { "type": "integer" },
                    "value":     { "type": "string" },
                    "location":  { "type": "string" }
                  },
                  "required": ["name", "timestamp", "value"]
                }
                """);

        assertThat(v2.isAssignableFrom(v1).isAssignable())
                .as("New schema adds optional field — old data without it is still valid")
                .isTrue();
    }

    @Test
    @DisplayName("Remove optional field — forward compatible")
    void removeOptionalField_forward_pass() {
        var v1 = toStruct("Event", """
                {
                  "type": "object",
                  "properties": {
                    "name":  { "type": "string" },
                    "color": { "type": "string" }
                  },
                  "required": ["name"]
                }
                """);
        var v2 = toStruct("Event", """
                {
                  "type": "object",
                  "properties": {
                    "name": { "type": "string" }
                  },
                  "required": ["name"]
                }
                """);

        assertThat(v1.isAssignableFrom(v2).isAssignable())
                .as("Old schema has optional 'color'; new schema drops it — old consumer can still read new data")
                .isTrue();
    }

    // ===== REQUIRED FIELDS =====

    @Test
    @DisplayName("Add required field — not backward compatible")
    void addRequiredField_backward_fail() {
        var v1 = toStruct("Event", """
                {
                  "type": "object",
                  "properties": {
                    "name":  { "type": "string" },
                    "value": { "type": "string" }
                  },
                  "required": ["name", "value"]
                }
                """);
        var v2 = toStruct("Event", """
                {
                  "type": "object",
                  "properties": {
                    "name":  { "type": "string" },
                    "value": { "type": "string" },
                    "unit":  { "type": "string" }
                  },
                  "required": ["name", "value", "unit"]
                }
                """);

        assertThat(v2.isAssignableFrom(v1).isAssignable())
                .as("New schema with required field must not be backward compatible — old data lacks the field")
                .isFalse();
    }

    @Test
    @DisplayName("Remove required field — not forward compatible")
    void removeRequiredField_forward_fail() {
        var v1 = toStruct("Event", """
                {
                  "type": "object",
                  "properties": {
                    "name":  { "type": "string" },
                    "value": { "type": "string" }
                  },
                  "required": ["name", "value"]
                }
                """);
        var v2 = toStruct("Event", """
                {
                  "type": "object",
                  "properties": {
                    "name": { "type": "string" }
                  },
                  "required": ["name"]
                }
                """);

        assertThat(v1.isAssignableFrom(v2).isAssignable())
                .as("Old schema requiring 'value' must not be forward compatible with new schema that dropped it")
                .isFalse();
    }

    // ===== FIELD OPTIONALITY CHANGES =====

    @Test
    @DisplayName("Optional → Required: not backward compatible — required field may be absent in old data")
    void optionalToRequired_backward_fail() {
        var v1 = toStruct("Event", """
                {
                  "type": "object",
                  "properties": {
                    "name":   { "type": "string" },
                    "status": { "type": "string" }
                  },
                  "required": ["name"]
                }
                """);
        var v2 = toStruct("Event", """
                {
                  "type": "object",
                  "properties": {
                    "name":   { "type": "string" },
                    "status": { "type": "string" }
                  },
                  "required": ["name", "status"]
                }
                """);

        // v2.isAssignableFrom(v1): v2 requires 'status' but v1 has it as optional — old data may lack the value
        assertThat(v2.isAssignableFrom(v1).isAssignable())
                .as("Not backward compatible — v2 requires 'status' but v1 treats it as optional")
                .isFalse();
    }

    @Test
    @DisplayName("Required → Optional: backward compatible")
    void requiredToOptional_backward_pass() {
        var v1 = toStruct("Event", """
                {
                  "type": "object",
                  "properties": {
                    "name":   { "type": "string" },
                    "status": { "type": "string" }
                  },
                  "required": ["name", "status"]
                }
                """);
        var v2 = toStruct("Event", """
                {
                  "type": "object",
                  "properties": {
                    "name":   { "type": "string" },
                    "status": { "type": "string" }
                  },
                  "required": ["name"]
                }
                """);

        assertThat(v2.isAssignableFrom(v1).isAssignable())
                .as("Making a field optional is backward compatible — old data always carries the value")
                .isTrue();
    }

    // ===== TYPE PROMOTION: NUMBER VS INTEGER =====

    @Test
    @DisplayName("JSON Schema has no AVRO-style type promotion; number (double) and integer are incompatible in KSML — backward direction fails")
    void numberVsInteger_backward_fail() {
        var v1 = toStruct("Event", """
                {
                  "type": "object",
                  "properties": {
                    "count": { "type": "integer" }
                  },
                  "required": ["count"]
                }
                """);
        var v2 = toStruct("Event", """
                {
                  "type": "object",
                  "properties": {
                    "count": { "type": "number" }
                  },
                  "required": ["count"]
                }
                """);

        assertThat(v2.isAssignableFrom(v1).isAssignable())
                .as("DOUBLE_SCHEMA.isAssignableFrom(INTEGER_SCHEMA) fails — number and integer are in different KSML type sets")
                .isFalse();
    }

    @Test
    @DisplayName("JSON Schema has no AVRO-style type promotion; number (double) and integer are incompatible in KSML — forward direction fails")
    void numberVsInteger_forward_fail() {
        var v1 = toStruct("Event", """
                {
                  "type": "object",
                  "properties": {
                    "count": { "type": "integer" }
                  },
                  "required": ["count"]
                }
                """);
        var v2 = toStruct("Event", """
                {
                  "type": "object",
                  "properties": {
                    "count": { "type": "number" }
                  },
                  "required": ["count"]
                }
                """);

        assertThat(v1.isAssignableFrom(v2).isAssignable())
                .as("INTEGER_SCHEMA.isAssignableFrom(DOUBLE_SCHEMA) fails — number and integer are in different KSML type sets")
                .isFalse();
    }

    // ===== TYPE INCOMPATIBLE: STRING ↔ INTEGER =====

    @Test
    @DisplayName("string → integer: not backward compatible")
    void stringToInteger_backward_fail() {
        var v1 = toStruct("Event", """
                {
                  "type": "object",
                  "properties": {
                    "value": { "type": "string" }
                  },
                  "required": ["value"]
                }
                """);
        var v2 = toStruct("Event", """
                {
                  "type": "object",
                  "properties": {
                    "value": { "type": "integer" }
                  },
                  "required": ["value"]
                }
                """);

        assertThat(v2.isAssignableFrom(v1).isAssignable())
                .as("Changing string to integer is not backward compatible")
                .isFalse();
    }

    @Test
    @DisplayName("string → integer: not forward compatible")
    void stringToInteger_forward_fail() {
        var v1 = toStruct("Event", """
                {
                  "type": "object",
                  "properties": {
                    "value": { "type": "string" }
                  },
                  "required": ["value"]
                }
                """);
        var v2 = toStruct("Event", """
                {
                  "type": "object",
                  "properties": {
                    "value": { "type": "integer" }
                  },
                  "required": ["value"]
                }
                """);

        assertThat(v1.isAssignableFrom(v2).isAssignable())
                .as("Changing string to integer is not forward compatible")
                .isFalse();
    }

    // ===== TYPE INCOMPATIBLE: INTEGER ↔ STRING =====

    @Test
    @DisplayName("integer → string: not backward compatible")
    void integerToString_backward_fail() {
        var v1 = toStruct("Event", """
                {
                  "type": "object",
                  "properties": {
                    "count": { "type": "integer" }
                  },
                  "required": ["count"]
                }
                """);
        var v2 = toStruct("Event", """
                {
                  "type": "object",
                  "properties": {
                    "count": { "type": "string" }
                  },
                  "required": ["count"]
                }
                """);

        assertThat(v2.isAssignableFrom(v1).isAssignable())
                .as("Changing integer to string is not backward compatible")
                .isFalse();
    }

    @Test
    @DisplayName("integer → string: not forward compatible")
    void integerToString_forward_fail() {
        var v1 = toStruct("Event", """
                {
                  "type": "object",
                  "properties": {
                    "count": { "type": "integer" }
                  },
                  "required": ["count"]
                }
                """);
        var v2 = toStruct("Event", """
                {
                  "type": "object",
                  "properties": {
                    "count": { "type": "string" }
                  },
                  "required": ["count"]
                }
                """);

        assertThat(v1.isAssignableFrom(v2).isAssignable())
                .as("Changing integer to string is not forward compatible")
                .isFalse();
    }

    // ===== DEFAULT VALUES =====

    @Test
    @DisplayName("Changing default value is a structural no-op at the KSML schema level")
    void changeDefaultValue_bothDirections_pass() {
        var v1 = toStruct("Event", """
                {
                  "type": "object",
                  "properties": {
                    "name":  { "type": "string" },
                    "color": { "type": "string", "default": "red" }
                  },
                  "required": ["name"]
                }
                """);
        var v2 = toStruct("Event", """
                {
                  "type": "object",
                  "properties": {
                    "name":  { "type": "string" },
                    "color": { "type": "string", "default": "blue" }
                  },
                  "required": ["name"]
                }
                """);

        assertThat(v2.isAssignableFrom(v1).isAssignable())
                .as("Changing default value must not break backward compatibility")
                .isTrue();
        assertThat(v1.isAssignableFrom(v2).isAssignable())
                .as("Changing default value must not break forward compatibility")
                .isTrue();
    }

    // ===== ENUM: ADD VALUE =====

    @Test
    @DisplayName("Add enum value — backward compatible (v1 symbols ⊆ v2 symbols)")
    void addEnumValue_backward_pass() {
        var v1 = toStruct("Reading", """
                {
                  "type": "object",
                  "properties": {
                    "sensorType": { "enum": ["A", "B", "C"] }
                  },
                  "required": ["sensorType"]
                }
                """);
        var v2 = toStruct("Reading", """
                {
                  "type": "object",
                  "properties": {
                    "sensorType": { "enum": ["A", "B", "C", "D"] }
                  },
                  "required": ["sensorType"]
                }
                """);

        assertThat(v2.isAssignableFrom(v1).isAssignable())
                .as("Adding a new enum value is backward compatible — v1 symbols are a subset of v2 symbols")
                .isTrue();
    }

    // ===== ENUM: REMOVE VALUE =====

    @Test
    @DisplayName("Remove enum value — not backward compatible: new schema may receive old data using the removed symbol")
    void removeEnumValue_backward_fail() {
        var v1 = toStruct("Reading", """
                {
                  "type": "object",
                  "properties": {
                    "sensorType": { "enum": ["A", "B", "C", "D"] }
                  },
                  "required": ["sensorType"]
                }
                """);
        var v2 = toStruct("Reading", """
                {
                  "type": "object",
                  "properties": {
                    "sensorType": { "enum": ["A", "B", "C"] }
                  },
                  "required": ["sensorType"]
                }
                """);

        // v2.isAssignableFrom(v1): EnumSchema(v2=[A,B,C]).isAssignableFrom(EnumSchema(v1=[A,B,C,D])) — D not in v2 → FAIL
        assertThat(v2.isAssignableFrom(v1).isAssignable())
                .as("Not backward compatible — old data may carry symbol D which v2 does not recognise")
                .isFalse();
    }

    // ===== ENUM: REORDER VALUES =====

    @Test
    @DisplayName("Reorder enum values — compatible in both directions (KSML checks set membership, not order)")
    void reorderEnumValues_bothDirections_pass() {
        var v1 = toStruct("Reading", """
                {
                  "type": "object",
                  "properties": {
                    "sensorType": { "enum": ["A", "B", "C"] }
                  },
                  "required": ["sensorType"]
                }
                """);
        var v2 = toStruct("Reading", """
                {
                  "type": "object",
                  "properties": {
                    "sensorType": { "enum": ["C", "B", "A"] }
                  },
                  "required": ["sensorType"]
                }
                """);

        assertThat(v2.isAssignableFrom(v1).isAssignable())
                .as("Reordering enum values must be backward compatible")
                .isTrue();
        assertThat(v1.isAssignableFrom(v2).isAssignable())
                .as("Reordering enum values must be forward compatible")
                .isTrue();
    }

    // ===== NESTED SCHEMA =====

    @Test
    @DisplayName("Add optional field to nested object — backward compatible")
    void addOptionalFieldToNestedObject_backward_pass() {
        var v1 = toStruct("Asset", """
                {
                  "type": "object",
                  "properties": {
                    "id": { "type": "string" },
                    "location": {
                      "type": "object",
                      "properties": {
                        "city": { "type": "string" }
                      },
                      "required": ["city"]
                    }
                  },
                  "required": ["id", "location"]
                }
                """);
        var v2 = toStruct("Asset", """
                {
                  "type": "object",
                  "properties": {
                    "id": { "type": "string" },
                    "location": {
                      "type": "object",
                      "properties": {
                        "city":    { "type": "string" },
                        "country": { "type": "string" }
                      },
                      "required": ["city"]
                    }
                  },
                  "required": ["id", "location"]
                }
                """);

        assertThat(v2.isAssignableFrom(v1).isAssignable())
                .as("Nested object gains an optional field — old data without it is still valid")
                .isTrue();
    }

    @Test
    @DisplayName("Change type of nested field — not compatible in either direction")
    void changeNestedFieldType_bothDirections_fail() {
        var v1 = toStruct("Asset", """
                {
                  "type": "object",
                  "properties": {
                    "id": { "type": "string" },
                    "location": {
                      "type": "object",
                      "properties": {
                        "lat": { "type": "string" }
                      },
                      "required": ["lat"]
                    }
                  },
                  "required": ["id", "location"]
                }
                """);
        var v2 = toStruct("Asset", """
                {
                  "type": "object",
                  "properties": {
                    "id": { "type": "string" },
                    "location": {
                      "type": "object",
                      "properties": {
                        "lat": { "type": "integer" }
                      },
                      "required": ["lat"]
                    }
                  },
                  "required": ["id", "location"]
                }
                """);

        assertThat(v2.isAssignableFrom(v1).isAssignable())
                .as("Type change in nested object must not be backward compatible")
                .isFalse();
        assertThat(v1.isAssignableFrom(v2).isAssignable())
                .as("Type change in nested object must not be forward compatible")
                .isFalse();
    }

    // ===== FIELD ORDER =====

    @Test
    @DisplayName("Field order is irrelevant in JSON Schema — compatible in both directions (KSML is name-based)")
    void reorderFields_bothDirections_pass() {
        var v1 = toStruct("Event", """
                {
                  "type": "object",
                  "properties": {
                    "name":      { "type": "string" },
                    "timestamp": { "type": "integer" },
                    "value":     { "type": "string" }
                  },
                  "required": ["name", "timestamp", "value"]
                }
                """);
        var v2 = toStruct("Event", """
                {
                  "type": "object",
                  "properties": {
                    "value":     { "type": "string" },
                    "name":      { "type": "string" },
                    "timestamp": { "type": "integer" }
                  },
                  "required": ["value", "name", "timestamp"]
                }
                """);

        assertThat(v2.isAssignableFrom(v1).isAssignable())
                .as("Field reorder must be backward compatible — KSML matches fields by name")
                .isTrue();
        assertThat(v1.isAssignableFrom(v2).isAssignable())
                .as("Field reorder must be forward compatible — KSML matches fields by name")
                .isTrue();
    }

}
