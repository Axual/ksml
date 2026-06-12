package io.axual.ksml.data.notation.protobuf;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - PROTOBUF
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

import com.squareup.wire.schema.Location;
import com.squareup.wire.schema.internal.parser.ProtoParser;
import io.axual.ksml.data.mapper.DataTypeDataSchemaMapper;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.schema.StructSchema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies KSML schema-evolution compatibility rules for Protobuf across 18 scenarios.
 * Convention: s2.isAssignableFrom(s1) = backward compat; s1.isAssignableFrom(s2) = forward compat.
 *
 * <p>Proto3 fields (no explicit label) and proto2 {@code optional} fields are treated as optional
 * in KSML (defaultValue = DataNull.INSTANCE). Only proto2 {@code required} fields are mandatory.
 * Scenarios that require a distinction between required and optional use proto2 syntax.</p>
 */
class ProtobufSchemaEvolutionCompatibilityTest {

    private static final String NS = "io.axual.ksml.test";
    private final ProtobufFileElementSchemaMapper mapper = new ProtobufFileElementSchemaMapper(
            new NativeDataObjectMapper(), new DataTypeDataSchemaMapper());

    private StructSchema toStruct(String name, String protoText) {
        var fileElement = ProtoParser.Companion.parse(Location.get(""), protoText);
        var result = mapper.toDataSchema(NS, name, fileElement);
        assertThat(result).isInstanceOf(StructSchema.class);
        return (StructSchema) result;
    }

    // ===== OPTIONAL FIELDS =====

    @Test
    @DisplayName("Add optional field — backward compatible")
    void addOptionalField_backward_pass() {
        var v1 = toStruct("Event", """
                syntax = "proto3";
                package io.axual.ksml.test;
                message Event {
                  string name = 1;
                  int64 timestamp = 2;
                  string value = 3;
                }
                """);
        var v2 = toStruct("Event", """
                syntax = "proto3";
                package io.axual.ksml.test;
                message Event {
                  string name = 1;
                  int64 timestamp = 2;
                  string value = 3;
                  string location = 4;
                }
                """);

        assertThat(v2.isAssignableFrom(v1).isAssignable())
                .as("New schema adds an optional field — old data without it is still valid")
                .isTrue();
    }

    @Test
    @DisplayName("Remove optional field — forward compatible")
    void removeOptionalField_forward_pass() {
        var v1 = toStruct("Event", """
                syntax = "proto3";
                package io.axual.ksml.test;
                message Event {
                  string name = 1;
                  string color = 2;
                }
                """);
        var v2 = toStruct("Event", """
                syntax = "proto3";
                package io.axual.ksml.test;
                message Event {
                  string name = 1;
                }
                """);

        assertThat(v1.isAssignableFrom(v2).isAssignable())
                .as("Old schema has optional 'color'; new schema drops it — old consumer can still read new data")
                .isTrue();
    }

    // ===== REQUIRED FIELDS (proto2 required label) =====

    @Test
    @DisplayName("Add required field — not backward compatible")
    void addRequiredField_backward_fail() {
        var v1 = toStruct("Event", """
                syntax = "proto2";
                package io.axual.ksml.test;
                message Event {
                  required string name = 1;
                  required string value = 2;
                }
                """);
        var v2 = toStruct("Event", """
                syntax = "proto2";
                package io.axual.ksml.test;
                message Event {
                  required string name = 1;
                  required string value = 2;
                  required string unit = 3;
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
                syntax = "proto2";
                package io.axual.ksml.test;
                message Event {
                  required string name = 1;
                  required string value = 2;
                }
                """);
        var v2 = toStruct("Event", """
                syntax = "proto2";
                package io.axual.ksml.test;
                message Event {
                  required string name = 1;
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
                syntax = "proto2";
                package io.axual.ksml.test;
                message Event {
                  required string name = 1;
                  optional string status = 2;
                }
                """);
        var v2 = toStruct("Event", """
                syntax = "proto2";
                package io.axual.ksml.test;
                message Event {
                  required string name = 1;
                  required string status = 2;
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
                syntax = "proto2";
                package io.axual.ksml.test;
                message Event {
                  required string name = 1;
                  required string status = 2;
                }
                """);
        var v2 = toStruct("Event", """
                syntax = "proto2";
                package io.axual.ksml.test;
                message Event {
                  required string name = 1;
                  optional string status = 2;
                }
                """);

        assertThat(v2.isAssignableFrom(v1).isAssignable())
                .as("Making a field optional is backward compatible — old data always carries the value")
                .isTrue();
    }

    // ===== TYPE PROMOTION: int32 → int64 =====

    @Test
    @DisplayName("int32 → int64 backward: int32 widens to int64 — backward compatible")
    void int32ToInt64_backward_pass() {
        var v1 = toStruct("Event", """
                syntax = "proto3";
                package io.axual.ksml.test;
                message Event {
                  int32 count = 1;
                }
                """);
        var v2 = toStruct("Event", """
                syntax = "proto3";
                package io.axual.ksml.test;
                message Event {
                  int64 count = 1;
                }
                """);

        assertThat(v2.isAssignableFrom(v1).isAssignable())
                .as("int32 widens to int64 without data loss — backward compatible")
                .isTrue();
    }

    @Test
    @DisplayName("int64 → int32 forward: narrowing — old int32 schema cannot safely read int64 values")
    void int64ToInt32_forward_fail() {
        var v1 = toStruct("Event", """
                syntax = "proto3";
                package io.axual.ksml.test;
                message Event {
                  int32 count = 1;
                }
                """);
        var v2 = toStruct("Event", """
                syntax = "proto3";
                package io.axual.ksml.test;
                message Event {
                  int64 count = 1;
                }
                """);

        assertThat(v1.isAssignableFrom(v2).isAssignable())
                .as("Narrowing: old int32 schema cannot safely read int64 values")
                .isFalse();
    }

    // ===== TYPE INCOMPATIBLE =====

    @Test
    @DisplayName("string → int32: not backward compatible")
    void stringToInt32_backward_fail() {
        var v1 = toStruct("Event", """
                syntax = "proto3";
                package io.axual.ksml.test;
                message Event {
                  string count = 1;
                }
                """);
        var v2 = toStruct("Event", """
                syntax = "proto3";
                package io.axual.ksml.test;
                message Event {
                  int32 count = 1;
                }
                """);

        assertThat(v2.isAssignableFrom(v1).isAssignable())
                .as("Changing string to int32 is not backward compatible")
                .isFalse();
    }

    @Test
    @DisplayName("string → int32: not forward compatible")
    void stringToInt32_forward_fail() {
        var v1 = toStruct("Event", """
                syntax = "proto3";
                package io.axual.ksml.test;
                message Event {
                  string count = 1;
                }
                """);
        var v2 = toStruct("Event", """
                syntax = "proto3";
                package io.axual.ksml.test;
                message Event {
                  int32 count = 1;
                }
                """);

        assertThat(v1.isAssignableFrom(v2).isAssignable())
                .as("Changing string to int32 is not forward compatible")
                .isFalse();
    }

    @Test
    @DisplayName("int32 → string: not backward compatible")
    void int32ToString_backward_fail() {
        var v1 = toStruct("Event", """
                syntax = "proto3";
                package io.axual.ksml.test;
                message Event {
                  int32 count = 1;
                }
                """);
        var v2 = toStruct("Event", """
                syntax = "proto3";
                package io.axual.ksml.test;
                message Event {
                  string count = 1;
                }
                """);

        assertThat(v2.isAssignableFrom(v1).isAssignable())
                .as("Changing int32 to string is not backward compatible")
                .isFalse();
    }

    @Test
    @DisplayName("int32 → string: not forward compatible")
    void int32ToString_forward_fail() {
        var v1 = toStruct("Event", """
                syntax = "proto3";
                package io.axual.ksml.test;
                message Event {
                  int32 count = 1;
                }
                """);
        var v2 = toStruct("Event", """
                syntax = "proto3";
                package io.axual.ksml.test;
                message Event {
                  string count = 1;
                }
                """);

        assertThat(v1.isAssignableFrom(v2).isAssignable())
                .as("Changing int32 to string is not forward compatible")
                .isFalse();
    }

    // ===== DEFAULT VALUES =====

    @Test
    @DisplayName("Change default value — no impact on structural compatibility, both directions pass")
    void changeDefaultValue_bothDirections_pass() {
        var v1 = toStruct("Event", """
                syntax = "proto2";
                package io.axual.ksml.test;
                message Event {
                  required string name = 1;
                  optional string status = 2 [default = "active"];
                }
                """);
        var v2 = toStruct("Event", """
                syntax = "proto2";
                package io.axual.ksml.test;
                message Event {
                  required string name = 1;
                  optional string status = 2 [default = "inactive"];
                }
                """);

        assertThat(v2.isAssignableFrom(v1).isAssignable())
                .as("Changing default value is structurally backward compatible")
                .isTrue();
        assertThat(v1.isAssignableFrom(v2).isAssignable())
                .as("Changing default value is structurally forward compatible")
                .isTrue();
    }

    // ===== ENUM =====

    @Test
    @DisplayName("Add enum value — backward compatible")
    void addEnumValue_backward_pass() {
        var v1 = toStruct("Event", """
                syntax = "proto3";
                package io.axual.ksml.test;
                enum Status { ACTIVE = 0; INACTIVE = 1; }
                message Event {
                  string name = 1;
                  Status status = 2;
                }
                """);
        var v2 = toStruct("Event", """
                syntax = "proto3";
                package io.axual.ksml.test;
                enum Status { ACTIVE = 0; INACTIVE = 1; PENDING = 2; }
                message Event {
                  string name = 1;
                  Status status = 2;
                }
                """);

        assertThat(v2.isAssignableFrom(v1).isAssignable())
                .as("Adding an enum value is backward compatible — old data only uses existing symbols")
                .isTrue();
    }

    @Test
    @DisplayName("Remove enum value — not backward compatible: new schema may receive old data using the removed symbol")
    void removeEnumValue_backward_fail() {
        var v1 = toStruct("Event", """
                syntax = "proto3";
                package io.axual.ksml.test;
                enum Status { ACTIVE = 0; INACTIVE = 1; PENDING = 2; }
                message Event {
                  string name = 1;
                  Status status = 2;
                }
                """);
        var v2 = toStruct("Event", """
                syntax = "proto3";
                package io.axual.ksml.test;
                enum Status { ACTIVE = 0; INACTIVE = 1; }
                message Event {
                  string name = 1;
                  Status status = 2;
                }
                """);

        // v2.isAssignableFrom(v1): EnumSchema(v2=[ACTIVE,INACTIVE]).isAssignableFrom(EnumSchema(v1=[ACTIVE,INACTIVE,PENDING])) — PENDING not in v2 → FAIL
        assertThat(v2.isAssignableFrom(v1).isAssignable())
                .as("Not backward compatible — old data may carry PENDING which v2 does not recognise")
                .isFalse();
    }

    @Test
    @DisplayName("Reorder enum values — NOT compatible in either direction: Protobuf encodes enums by tag number, "
            + "so renumbering ACTIVE(0→1) breaks old data that encoded ACTIVE as 0")
    void reorderEnumValues_bothDirections_fail() {
        var v1 = toStruct("Event", """
                syntax = "proto3";
                package io.axual.ksml.test;
                enum Status { ACTIVE = 0; INACTIVE = 1; }
                message Event {
                  string name = 1;
                  Status status = 2;
                }
                """);
        var v2 = toStruct("Event", """
                syntax = "proto3";
                package io.axual.ksml.test;
                enum Status { INACTIVE = 0; ACTIVE = 1; }
                message Event {
                  string name = 1;
                  Status status = 2;
                }
                """);

        assertThat(v2.isAssignableFrom(v1).isAssignable())
                .as("Reordering enum values changes wire tag numbers — NOT backward compatible in Protobuf")
                .isFalse();
        assertThat(v1.isAssignableFrom(v2).isAssignable())
                .as("Reordering enum values changes wire tag numbers — NOT forward compatible in Protobuf")
                .isFalse();
    }

    // ===== NESTED SCHEMA =====

    @Test
    @DisplayName("Add optional field to nested message — backward compatible")
    void addOptionalFieldToNestedMessage_backward_pass() {
        var v1 = toStruct("Asset", """
                syntax = "proto3";
                package io.axual.ksml.test;
                message Location {
                  string city = 1;
                }
                message Asset {
                  string id = 1;
                  Location location = 2;
                }
                """);
        var v2 = toStruct("Asset", """
                syntax = "proto3";
                package io.axual.ksml.test;
                message Location {
                  string city = 1;
                  string country = 2;
                }
                message Asset {
                  string id = 1;
                  Location location = 2;
                }
                """);

        assertThat(v2.isAssignableFrom(v1).isAssignable())
                .as("Nested message gains an optional field — old data without it is still valid")
                .isTrue();
    }

    @Test
    @DisplayName("Change type of nested field — not compatible in either direction")
    void changeNestedFieldType_bothDirections_fail() {
        var v1 = toStruct("Asset", """
                syntax = "proto3";
                package io.axual.ksml.test;
                message Location {
                  string city = 1;
                }
                message Asset {
                  string id = 1;
                  Location location = 2;
                }
                """);
        var v2 = toStruct("Asset", """
                syntax = "proto3";
                package io.axual.ksml.test;
                message Location {
                  int32 city = 1;
                }
                message Asset {
                  string id = 1;
                  Location location = 2;
                }
                """);

        assertThat(v2.isAssignableFrom(v1).isAssignable())
                .as("Nested field type change must not be backward compatible")
                .isFalse();
        assertThat(v1.isAssignableFrom(v2).isAssignable())
                .as("Nested field type change must not be forward compatible")
                .isFalse();
    }

    // ===== FIELD ORDER =====

    @Test
    @DisplayName("Reorder fields — compatible in both directions (KSML is name-based)")
    void reorderFields_bothDirections_pass() {
        var v1 = toStruct("Event", """
                syntax = "proto3";
                package io.axual.ksml.test;
                message Event {
                  string name = 1;
                  int64 timestamp = 2;
                  string value = 3;
                }
                """);
        var v2 = toStruct("Event", """
                syntax = "proto3";
                package io.axual.ksml.test;
                message Event {
                  int64 timestamp = 1;
                  string value = 2;
                  string name = 3;
                }
                """);

        assertThat(v2.isAssignableFrom(v1).isAssignable())
                .as("Reordering fields must be backward compatible — KSML matches by name")
                .isTrue();
        assertThat(v1.isAssignableFrom(v2).isAssignable())
                .as("Reordering fields must be forward compatible — KSML matches by name")
                .isTrue();
    }

    // ===== UNION (oneOf) =====

    @Test
    @DisplayName("Add member to oneOf — backward compatible")
    void addOneOfMember_backward_pass() {
        var v1 = toStruct("Event", """
                syntax = "proto3";
                package io.axual.ksml.test;
                message Event {
                  string name = 1;
                  oneof payload {
                    string text = 2;
                    int32 count = 3;
                  }
                }
                """);
        var v2 = toStruct("Event", """
                syntax = "proto3";
                package io.axual.ksml.test;
                message Event {
                  string name = 1;
                  oneof payload {
                    string text = 2;
                    int32 count = 3;
                    double ratio = 4;
                  }
                }
                """);

        assertThat(v2.isAssignableFrom(v1).isAssignable())
                .as("Adding a oneOf member is backward compatible — new reader handles all old values")
                .isTrue();
    }

    @Test
    @DisplayName("Remove member from oneOf — not backward compatible: new schema cannot handle old data that used the removed member")
    void removeOneOfMember_backward_fail() {
        var v1 = toStruct("Event", """
                syntax = "proto3";
                package io.axual.ksml.test;
                message Event {
                  string name = 1;
                  oneof payload {
                    string text = 2;
                    int32 count = 3;
                    double ratio = 4;
                  }
                }
                """);
        var v2 = toStruct("Event", """
                syntax = "proto3";
                package io.axual.ksml.test;
                message Event {
                  string name = 1;
                  oneof payload {
                    string text = 2;
                    int32 count = 3;
                  }
                }
                """);

        // v2.isAssignableFrom(v1): UnionSchema(v2=[text,count]).isAssignableFrom(UnionSchema(v1=[text,count,ratio])) — ratio not in v2 → FAIL
        assertThat(v2.isAssignableFrom(v1).isAssignable())
                .as("Not backward compatible — old data may carry a ratio value that v2 cannot handle")
                .isFalse();
    }
}
