package io.axual.ksml.data.notation.avro;

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
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies KSML schema-evolution compatibility rules across 18 scenarios.
 * Convention: s2.isAssignableFrom(s1) = backward compat; s1.isAssignableFrom(s2) = forward compat.
 */
class AvroSchemaEvolutionCompatibilityTest {

    private static final String NS = "io.axual.ksml.test";
    private final AvroSchemaMapper mapper = new AvroSchemaMapper();

    private StructSchema toStruct(Schema avroSchema) {
        var result = mapper.toDataSchema(avroSchema.getNamespace(), avroSchema.getName(), avroSchema);
        assertThat(result).isInstanceOf(StructSchema.class);
        return (StructSchema) result;
    }

    // ===== OPTIONAL FIELDS =====

    @Test
    @DisplayName("Add optional field — backward compatible (new consumer reads old data)")
    void addOptionalField_backward_pass() {
        var v1 = SchemaBuilder.record("Event").namespace(NS).fields()
                .requiredString("name")
                .requiredLong("timestamp")
                .requiredString("value")
                .endRecord();
        var v2 = SchemaBuilder.record("Event").namespace(NS).fields()
                .requiredString("name")
                .requiredLong("timestamp")
                .requiredString("value")
                .optionalString("location")
                .endRecord();

        var s1 = toStruct(v1);
        var s2 = toStruct(v2);

        assertThat(s2.isAssignableFrom(s1).isAssignable())
                .as("New schema with optional field must be backward compatible with old schema")
                .isTrue();
    }

    @Test
    @DisplayName("Remove optional field — forward compatible (old consumer reads new data)")
    void removeOptionalField_forward_pass() {
        var v1 = SchemaBuilder.record("Event").namespace(NS).fields()
                .requiredString("name")
                .optionalString("color")
                .endRecord();
        var v2 = SchemaBuilder.record("Event").namespace(NS).fields()
                .requiredString("name")
                .endRecord();

        var s1 = toStruct(v1);
        var s2 = toStruct(v2);

        assertThat(s1.isAssignableFrom(s2).isAssignable())
                .as("Old schema with optional field must be forward compatible with new schema that dropped it")
                .isTrue();
    }

    // ===== REQUIRED FIELDS =====

    @Test
    @DisplayName("Add required field — not backward compatible")
    void addRequiredField_backward_fail() {
        var v1 = SchemaBuilder.record("Event").namespace(NS).fields()
                .requiredString("name")
                .requiredString("value")
                .endRecord();
        var v2 = SchemaBuilder.record("Event").namespace(NS).fields()
                .requiredString("name")
                .requiredString("value")
                .requiredString("unit")
                .endRecord();

        var s1 = toStruct(v1);
        var s2 = toStruct(v2);

        assertThat(s2.isAssignableFrom(s1).isAssignable())
                .as("New schema with required field must not be backward compatible — old data lacks the field")
                .isFalse();
    }

    @Test
    @DisplayName("Remove required field — not forward compatible")
    void removeRequiredField_forward_fail() {
        var v1 = SchemaBuilder.record("Event").namespace(NS).fields()
                .requiredString("name")
                .requiredString("value")
                .endRecord();
        var v2 = SchemaBuilder.record("Event").namespace(NS).fields()
                .requiredString("name")
                .endRecord();

        var s1 = toStruct(v1);
        var s2 = toStruct(v2);

        assertThat(s1.isAssignableFrom(s2).isAssignable())
                .as("Old schema requiring 'value' must not be forward compatible with new schema that dropped it")
                .isFalse();
    }

    // ===== FIELD OPTIONALITY CHANGES =====

    @Test
    @DisplayName("Optional → Required: not backward compatible — required field may be absent in old data")
    void optionalToRequired_backward_fail() {
        // v1: status is optional (union null|string); v2: status is required string
        var v1 = SchemaBuilder.record("Event").namespace(NS).fields()
                .requiredString("name")
                .optionalString("status")
                .endRecord();
        var v2 = SchemaBuilder.record("Event").namespace(NS).fields()
                .requiredString("name")
                .requiredString("status")
                .endRecord();

        var s1 = toStruct(v1);
        var s2 = toStruct(v2);

        // s2.isAssignableFrom(s1): s2 requires 'status' but s1 has it as optional — old data may lack the value
        assertThat(s2.isAssignableFrom(s1).isAssignable())
                .as("Not backward compatible — s2 requires 'status' but s1 treats it as optional")
                .isFalse();
    }

    @Test
    @DisplayName("Required → Optional: backward compatible")
    void requiredToOptional_backward_pass() {
        var v1 = SchemaBuilder.record("Event").namespace(NS).fields()
                .requiredString("name")
                .requiredString("status")
                .endRecord();
        var v2 = SchemaBuilder.record("Event").namespace(NS).fields()
                .requiredString("name")
                .optionalString("status")
                .endRecord();

        var s1 = toStruct(v1);
        var s2 = toStruct(v2);

        assertThat(s2.isAssignableFrom(s1).isAssignable())
                .as("Making a field optional is backward compatible — old data always carries the value")
                .isTrue();
    }

    // ===== TYPE PROMOTION: INT → LONG =====

    @Test
    @DisplayName("int → long backward: int widens to long — backward compatible")
    void intToLong_backward_pass() {
        var v1 = SchemaBuilder.record("Event").namespace(NS).fields()
                .requiredInt("count")
                .endRecord();
        var v2 = SchemaBuilder.record("Event").namespace(NS).fields()
                .requiredLong("count")
                .endRecord();

        var s1 = toStruct(v1);
        var s2 = toStruct(v2);

        assertThat(s2.isAssignableFrom(s1).isAssignable())
                .as("int widens to long without data loss — backward compatible")
                .isTrue();
    }

    @Test
    @DisplayName("long → int forward: narrowing — old int schema cannot safely read long values")
    void intToLong_forward_fail() {
        var v1 = SchemaBuilder.record("Event").namespace(NS).fields()
                .requiredInt("count")
                .endRecord();
        var v2 = SchemaBuilder.record("Event").namespace(NS).fields()
                .requiredLong("count")
                .endRecord();

        var s1 = toStruct(v1);
        var s2 = toStruct(v2);

        assertThat(s1.isAssignableFrom(s2).isAssignable())
                .as("Narrowing: old int schema cannot safely read long values")
                .isFalse();
    }

    // ===== TYPE INCOMPATIBLE: STRING ↔ INT =====

    @Test
    @DisplayName("string → int: not backward compatible")
    void stringToInt_backward_fail() {
        var v1 = SchemaBuilder.record("Event").namespace(NS).fields()
                .requiredString("value")
                .endRecord();
        var v2 = SchemaBuilder.record("Event").namespace(NS).fields()
                .requiredInt("value")
                .endRecord();

        var s1 = toStruct(v1);
        var s2 = toStruct(v2);

        assertThat(s2.isAssignableFrom(s1).isAssignable())
                .as("Changing string to int is not backward compatible")
                .isFalse();
    }

    @Test
    @DisplayName("string → int: not forward compatible")
    void stringToInt_forward_fail() {
        var v1 = SchemaBuilder.record("Event").namespace(NS).fields()
                .requiredString("value")
                .endRecord();
        var v2 = SchemaBuilder.record("Event").namespace(NS).fields()
                .requiredInt("value")
                .endRecord();

        var s1 = toStruct(v1);
        var s2 = toStruct(v2);

        assertThat(s1.isAssignableFrom(s2).isAssignable())
                .as("Changing string to int is not forward compatible")
                .isFalse();
    }

    // ===== TYPE INCOMPATIBLE: INT → STRING =====

    @Test
    @DisplayName("int → string: not backward compatible")
    void intToString_backward_fail() {
        var v1 = SchemaBuilder.record("Event").namespace(NS).fields()
                .requiredInt("count")
                .endRecord();
        var v2 = SchemaBuilder.record("Event").namespace(NS).fields()
                .requiredString("count")
                .endRecord();

        var s1 = toStruct(v1);
        var s2 = toStruct(v2);

        assertThat(s2.isAssignableFrom(s1).isAssignable())
                .as("Changing int to string is not backward compatible")
                .isFalse();
    }

    @Test
    @DisplayName("int → string: not forward compatible")
    void intToString_forward_fail() {
        var v1 = SchemaBuilder.record("Event").namespace(NS).fields()
                .requiredInt("count")
                .endRecord();
        var v2 = SchemaBuilder.record("Event").namespace(NS).fields()
                .requiredString("count")
                .endRecord();

        var s1 = toStruct(v1);
        var s2 = toStruct(v2);

        assertThat(s1.isAssignableFrom(s2).isAssignable())
                .as("Changing int to string is not forward compatible")
                .isFalse();
    }

    // ===== DEFAULT VALUES =====

    @Test
    @DisplayName("Changing default value is structural no-op at KSML schema level")
    void changeDefaultValue_bothDirections_pass() {
        // Schema.Parser is used to control default values precisely
        var v1 = new Schema.Parser().parse("""
                {
                  "type": "record",
                  "name": "Event",
                  "namespace": "io.axual.ksml.test",
                  "fields": [
                    { "name": "name", "type": "string" },
                    { "name": "color", "type": ["null", "string"], "default": null }
                  ]
                }
                """);
        var v2 = new Schema.Parser().parse("""
                {
                  "type": "record",
                  "name": "Event",
                  "namespace": "io.axual.ksml.test",
                  "fields": [
                    { "name": "name", "type": "string" },
                    { "name": "color", "type": ["null", "string"], "default": null }
                  ]
                }
                """);

        var s1 = toStruct(v1);
        var s2 = toStruct(v2);

        assertThat(s2.isAssignableFrom(s1).isAssignable())
                .as("Changing default value must not break backward compatibility")
                .isTrue();
        assertThat(s1.isAssignableFrom(s2).isAssignable())
                .as("Changing default value must not break forward compatibility")
                .isTrue();
    }

    // ===== ENUM: ADD VALUE =====

    @Test
    @DisplayName("Add enum value with default — backward compatible (v1 symbols ⊆ v2 symbols)")
    void addEnumValue_backward_pass() {
        // Schema.Parser for precise enum control including default symbol
        var v1 = new Schema.Parser().parse("""
                {
                  "type": "record",
                  "name": "Reading",
                  "namespace": "io.axual.ksml.test",
                  "fields": [
                    {
                      "name": "sensorType",
                      "type": {
                        "type": "enum",
                        "name": "SensorType",
                        "namespace": "io.axual.ksml.test",
                        "symbols": ["A", "B", "C"]
                      }
                    }
                  ]
                }
                """);
        var v2 = new Schema.Parser().parse("""
                {
                  "type": "record",
                  "name": "Reading",
                  "namespace": "io.axual.ksml.test",
                  "fields": [
                    {
                      "name": "sensorType",
                      "type": {
                        "type": "enum",
                        "name": "SensorType",
                        "namespace": "io.axual.ksml.test",
                        "symbols": ["A", "B", "C", "D"],
                        "default": "A"
                      }
                    }
                  ]
                }
                """);

        var s1 = toStruct(v1);
        var s2 = toStruct(v2);

        // s2.isAssignableFrom(s1): for field sensorType, EnumSchema(v2).isAssignableFrom(EnumSchema(v1))
        // checks every symbol in v1 is in v2 → [A,B,C] ⊆ [A,B,C,D] → PASS
        assertThat(s2.isAssignableFrom(s1).isAssignable())
                .as("Adding a new enum value with a default is backward compatible")
                .isTrue();
    }

    // ===== ENUM: REMOVE VALUE =====

    @Test
    @DisplayName("Remove enum value — not backward compatible: new schema may receive old data using the removed symbol")
    void removeEnumValue_backward_fail() {
        var v1 = new Schema.Parser().parse("""
                {
                  "type": "record",
                  "name": "Reading",
                  "namespace": "io.axual.ksml.test",
                  "fields": [
                    {
                      "name": "sensorType",
                      "type": {
                        "type": "enum",
                        "name": "SensorType",
                        "namespace": "io.axual.ksml.test",
                        "symbols": ["A", "B", "C", "D"]
                      }
                    }
                  ]
                }
                """);
        var v2 = new Schema.Parser().parse("""
                {
                  "type": "record",
                  "name": "Reading",
                  "namespace": "io.axual.ksml.test",
                  "fields": [
                    {
                      "name": "sensorType",
                      "type": {
                        "type": "enum",
                        "name": "SensorType",
                        "namespace": "io.axual.ksml.test",
                        "symbols": ["A", "B", "C"]
                      }
                    }
                  ]
                }
                """);

        var s1 = toStruct(v1);
        var s2 = toStruct(v2);

        // s2.isAssignableFrom(s1): EnumSchema(v2=[A,B,C]).isAssignableFrom(EnumSchema(v1=[A,B,C,D])) — D not in v2 → FAIL
        assertThat(s2.isAssignableFrom(s1).isAssignable())
                .as("Not backward compatible — old data may carry symbol D which v2 does not recognise")
                .isFalse();
    }

    // ===== ENUM: REORDER VALUES =====

    @Test
    @DisplayName("Reorder enum values — compatible in both directions (KSML checks set membership, not order)")
    void reorderEnumValues_bothDirections_pass() {
        var v1 = new Schema.Parser().parse("""
                {
                  "type": "record",
                  "name": "Reading",
                  "namespace": "io.axual.ksml.test",
                  "fields": [
                    {
                      "name": "sensorType",
                      "type": {
                        "type": "enum",
                        "name": "SensorType",
                        "namespace": "io.axual.ksml.test",
                        "symbols": ["A", "B", "C"]
                      }
                    }
                  ]
                }
                """);
        var v2 = new Schema.Parser().parse("""
                {
                  "type": "record",
                  "name": "Reading",
                  "namespace": "io.axual.ksml.test",
                  "fields": [
                    {
                      "name": "sensorType",
                      "type": {
                        "type": "enum",
                        "name": "SensorType",
                        "namespace": "io.axual.ksml.test",
                        "symbols": ["C", "B", "A"]
                      }
                    }
                  ]
                }
                """);

        var s1 = toStruct(v1);
        var s2 = toStruct(v2);

        assertThat(s2.isAssignableFrom(s1).isAssignable())
                .as("Reordering enum values must be backward compatible")
                .isTrue();
        assertThat(s1.isAssignableFrom(s2).isAssignable())
                .as("Reordering enum values must be forward compatible")
                .isTrue();
    }

    // ===== NESTED SCHEMA =====

    @Test
    @DisplayName("Add optional field to nested record — backward compatible")
    void addOptionalFieldToNestedRecord_backward_pass() {
        var v1 = new Schema.Parser().parse("""
                {
                  "type": "record",
                  "name": "Asset",
                  "namespace": "io.axual.ksml.test",
                  "fields": [
                    { "name": "id", "type": "string" },
                    {
                      "name": "location",
                      "type": {
                        "type": "record",
                        "name": "Location",
                        "fields": [
                          { "name": "city", "type": "string" }
                        ]
                      }
                    }
                  ]
                }
                """);
        var v2 = new Schema.Parser().parse("""
                {
                  "type": "record",
                  "name": "Asset",
                  "namespace": "io.axual.ksml.test",
                  "fields": [
                    { "name": "id", "type": "string" },
                    {
                      "name": "location",
                      "type": {
                        "type": "record",
                        "name": "Location",
                        "fields": [
                          { "name": "city", "type": "string" },
                          { "name": "country", "type": ["null", "string"], "default": null }
                        ]
                      }
                    }
                  ]
                }
                """);

        var s1 = toStruct(v1);
        var s2 = toStruct(v2);

        assertThat(s2.isAssignableFrom(s1).isAssignable())
                .as("Adding an optional field to a nested record must be backward compatible")
                .isTrue();
    }

    @Test
    @DisplayName("Change type of nested record field — not compatible in either direction")
    void changeNestedFieldType_bothDirections_fail() {
        var v1 = new Schema.Parser().parse("""
                {
                  "type": "record",
                  "name": "Asset",
                  "namespace": "io.axual.ksml.test",
                  "fields": [
                    { "name": "id", "type": "string" },
                    {
                      "name": "location",
                      "type": {
                        "type": "record",
                        "name": "Location",
                        "fields": [
                          { "name": "lat", "type": "string" }
                        ]
                      }
                    }
                  ]
                }
                """);
        var v2 = new Schema.Parser().parse("""
                {
                  "type": "record",
                  "name": "Asset",
                  "namespace": "io.axual.ksml.test",
                  "fields": [
                    { "name": "id", "type": "string" },
                    {
                      "name": "location",
                      "type": {
                        "type": "record",
                        "name": "Location",
                        "fields": [
                          { "name": "lat", "type": "int" }
                        ]
                      }
                    }
                  ]
                }
                """);

        var s1 = toStruct(v1);
        var s2 = toStruct(v2);

        assertThat(s2.isAssignableFrom(s1).isAssignable())
                .as("Type change in nested record must not be backward compatible")
                .isFalse();
        assertThat(s1.isAssignableFrom(s2).isAssignable())
                .as("Type change in nested record must not be forward compatible")
                .isFalse();
    }

    // ===== FIELD ORDER =====

    @Test
    @DisplayName("Reorder top-level fields — compatible in both directions (KSML is name-based)")
    void reorderFields_bothDirections_pass() {
        var v1 = SchemaBuilder.record("Event").namespace(NS).fields()
                .requiredString("name")
                .requiredLong("timestamp")
                .requiredString("value")
                .endRecord();
        var v2 = SchemaBuilder.record("Event").namespace(NS).fields()
                .requiredString("value")
                .requiredString("name")
                .requiredLong("timestamp")
                .endRecord();

        var s1 = toStruct(v1);
        var s2 = toStruct(v2);

        assertThat(s2.isAssignableFrom(s1).isAssignable())
                .as("Field reorder must be backward compatible — KSML matches fields by name")
                .isTrue();
        assertThat(s1.isAssignableFrom(s2).isAssignable())
                .as("Field reorder must be forward compatible — KSML matches fields by name")
                .isTrue();
    }

    // ===== UNION: ADD TYPE =====

    @Test
    @DisplayName("Add type to union — backward compatible (v1 members ⊆ v2 members)")
    void addUnionType_backward_pass() {
        // Schema.Parser for union field definitions
        var v1 = new Schema.Parser().parse("""
                {
                  "type": "record",
                  "name": "Event",
                  "namespace": "io.axual.ksml.test",
                  "fields": [
                    { "name": "id", "type": "string" },
                    { "name": "reading", "type": ["string", "int"] }
                  ]
                }
                """);
        var v2 = new Schema.Parser().parse("""
                {
                  "type": "record",
                  "name": "Event",
                  "namespace": "io.axual.ksml.test",
                  "fields": [
                    { "name": "id", "type": "string" },
                    { "name": "reading", "type": ["string", "int", "boolean"] }
                  ]
                }
                """);

        var s1 = toStruct(v1);
        var s2 = toStruct(v2);

        // s2.isAssignableFrom(s1): UnionSchema(v2).isAssignableFrom(UnionSchema(v1))
        // for each member of v1 (string, int), check that v2 can handle it → both present → PASS
        assertThat(s2.isAssignableFrom(s1).isAssignable())
                .as("Adding a union type is backward compatible — v1 members are a subset of v2 members")
                .isTrue();
    }

    // ===== UNION: REMOVE TYPE =====

    @Test
    @DisplayName("Remove type from union — not backward compatible: new schema cannot handle old data that used the removed type")
    void removeUnionType_backward_fail() {
        var v1 = new Schema.Parser().parse("""
                {
                  "type": "record",
                  "name": "Event",
                  "namespace": "io.axual.ksml.test",
                  "fields": [
                    { "name": "id", "type": "string" },
                    { "name": "reading", "type": ["string", "int", "boolean"] }
                  ]
                }
                """);
        var v2 = new Schema.Parser().parse("""
                {
                  "type": "record",
                  "name": "Event",
                  "namespace": "io.axual.ksml.test",
                  "fields": [
                    { "name": "id", "type": "string" },
                    { "name": "reading", "type": ["string", "int"] }
                  ]
                }
                """);

        var s1 = toStruct(v1);
        var s2 = toStruct(v2);

        // s2.isAssignableFrom(s1): UnionSchema(v2=[string,int]).isAssignableFrom(UnionSchema(v1=[string,int,boolean]))
        // boolean is not in v2 → FAIL
        assertThat(s2.isAssignableFrom(s1).isAssignable())
                .as("Not backward compatible — old data may carry a boolean value that v2 cannot handle")
                .isFalse();
    }
}
