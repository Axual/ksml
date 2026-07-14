package io.axual.ksml.data.notation.protobuf;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - PROTOBUF
 * %%
 * Copyright (C) 2021 - 2026 Axual B.V.
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

import com.squareup.wire.schema.Field;
import com.squareup.wire.schema.Location;
import com.squareup.wire.schema.internal.parser.EnumElement;
import com.squareup.wire.schema.internal.parser.MessageElement;
import com.squareup.wire.schema.internal.parser.ProtoParser;
import io.axual.ksml.data.exception.SchemaException;
import io.axual.ksml.data.mapper.DataTypeDataSchemaMapper;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.EnumSchema;
import io.axual.ksml.data.schema.ListSchema;
import io.axual.ksml.data.schema.MapSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.schema.UnionSchema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProtobufFileElementSchemaMapperTest {

    private static final String NS = "io.axual.ksml.test";
    private final ProtobufFileElementSchemaMapper mapper = new ProtobufFileElementSchemaMapper(
            new NativeDataObjectMapper(), new DataTypeDataSchemaMapper());

    private StructSchema toStruct(String name, String protoText) {
        final var fileElement = ProtoParser.Companion.parse(Location.get(""), protoText);
        final var result = mapper.toDataSchema(NS, name, fileElement);
        assertThat(result).isInstanceOf(StructSchema.class);
        return (StructSchema) result;
    }

    // ===== toDataSchema: primitives =====

    @Test
    @DisplayName("All KSML-proto primitive types map to their corresponding KSML data schemas")
    void toDataSchema_primitiveTypes_allMappedCorrectly() {
        final var struct = toStruct("Primitives", """
                syntax = "proto3";
                package io.axual.ksml.test;
                message Primitives {
                  boolean flag = 1;
                  int32 i32 = 2;
                  int64 i64 = 3;
                  float f = 4;
                  double d = 5;
                  string s = 6;
                  bytes b = 7;
                }
                """);

        assertThat(struct.name()).isEqualTo("Primitives");
        assertThat(struct.namespace()).isEqualTo(NS);
        assertThat(struct.fields()).hasSize(7);
        assertThat(struct.field("flag").schema()).isEqualTo(DataSchema.BOOLEAN_SCHEMA);
        assertThat(struct.field("i32").schema()).isEqualTo(DataSchema.INTEGER_SCHEMA);
        assertThat(struct.field("i64").schema()).isEqualTo(DataSchema.LONG_SCHEMA);
        assertThat(struct.field("f").schema()).isEqualTo(DataSchema.FLOAT_SCHEMA);
        assertThat(struct.field("d").schema()).isEqualTo(DataSchema.DOUBLE_SCHEMA);
        assertThat(struct.field("s").schema()).isEqualTo(DataSchema.STRING_SCHEMA);
        assertThat(struct.field("b").schema()).isEqualTo(DataSchema.BYTES_SCHEMA);
    }

    @Test
    @DisplayName("All int32-equivalent proto types map to INTEGER schema")
    void toDataSchema_int32Variants_mapToIntegerSchema() {
        final var struct = toStruct("IntVariants", """
                syntax = "proto3";
                package io.axual.ksml.test;
                message IntVariants {
                  fixed32 f32 = 1;
                  sfixed32 sf32 = 2;
                  sint32 s32 = 3;
                  uint32 u32 = 4;
                }
                """);

        assertThat(struct.field("f32").schema()).isEqualTo(DataSchema.INTEGER_SCHEMA);
        assertThat(struct.field("sf32").schema()).isEqualTo(DataSchema.INTEGER_SCHEMA);
        assertThat(struct.field("s32").schema()).isEqualTo(DataSchema.INTEGER_SCHEMA);
        assertThat(struct.field("u32").schema()).isEqualTo(DataSchema.INTEGER_SCHEMA);
    }

    @Test
    @DisplayName("All int64-equivalent proto types map to LONG schema")
    void toDataSchema_int64Variants_mapToLongSchema() {
        final var struct = toStruct("LongVariants", """
                syntax = "proto3";
                package io.axual.ksml.test;
                message LongVariants {
                  int64 i64 = 1;
                  fixed64 f64 = 2;
                  sfixed64 sf64 = 3;
                  sint64 s64 = 4;
                  uint64 u64 = 5;
                }
                """);

        assertThat(struct.field("i64").schema()).isEqualTo(DataSchema.LONG_SCHEMA);
        assertThat(struct.field("f64").schema()).isEqualTo(DataSchema.LONG_SCHEMA);
        assertThat(struct.field("sf64").schema()).isEqualTo(DataSchema.LONG_SCHEMA);
        assertThat(struct.field("s64").schema()).isEqualTo(DataSchema.LONG_SCHEMA);
        assertThat(struct.field("u64").schema()).isEqualTo(DataSchema.LONG_SCHEMA);
    }

    // ===== toDataSchema: collections =====

    @Test
    @DisplayName("Repeated field maps to ListSchema wrapping the element schema")
    void toDataSchema_repeatedField_mapsToListSchema() {
        final var struct = toStruct("WithList", """
                syntax = "proto3";
                package io.axual.ksml.test;
                message WithList {
                  repeated string tags = 1;
                  repeated int32 counts = 2;
                }
                """);

        assertThat(struct.field("tags").schema()).isInstanceOf(ListSchema.class);
        assertThat(((ListSchema) struct.field("tags").schema()).valueSchema()).isEqualTo(DataSchema.STRING_SCHEMA);
        assertThat(struct.field("counts").schema()).isInstanceOf(ListSchema.class);
        assertThat(((ListSchema) struct.field("counts").schema()).valueSchema()).isEqualTo(DataSchema.INTEGER_SCHEMA);
    }

    // ===== toDataSchema: union/oneof =====

    @Test
    @DisplayName("oneOf block maps to UnionSchema with correctly named and typed branches")
    void toDataSchema_oneOf_mapsToUnionSchemaWithCorrectMembers() {
        final var struct = toStruct("WithOneOf", """
                syntax = "proto3";
                package io.axual.ksml.test;
                message WithOneOf {
                  string name = 1;
                  oneof payload {
                    string text = 2;
                    int32 count = 3;
                  }
                }
                """);

        assertThat(struct.fields()).hasSize(2);
        assertThat(struct.field("name").schema()).isEqualTo(DataSchema.STRING_SCHEMA);

        final var payloadField = struct.field("payload");
        assertThat(payloadField).isNotNull();
        assertThat(payloadField.schema()).isInstanceOf(UnionSchema.class);
        final var union = (UnionSchema) payloadField.schema();
        assertThat(union.members()).hasSize(2);

        final var memberNames = Arrays.stream(union.members()).map(UnionSchema.Member::name).toList();
        assertThat(memberNames).containsExactlyInAnyOrder("text", "count");

        final var textMember = Arrays.stream(union.members()).filter(m -> "text".equals(m.name())).findFirst();
        assertThat(textMember).isPresent();
        assertThat(textMember.get().schema()).isEqualTo(DataSchema.STRING_SCHEMA);

        final var countMember = Arrays.stream(union.members()).filter(m -> "count".equals(m.name())).findFirst();
        assertThat(countMember).isPresent();
        assertThat(countMember.get().schema()).isEqualTo(DataSchema.INTEGER_SCHEMA);
    }

    // ===== toDataSchema: enum =====

    @Test
    @DisplayName("Enum reference maps to EnumSchema with all symbols in order")
    void toDataSchema_enumField_mapsToEnumSchemaWithAllSymbols() {
        final var struct = toStruct("WithEnum", """
                syntax = "proto3";
                package io.axual.ksml.test;
                enum Status { ACTIVE = 0; INACTIVE = 1; PENDING = 2; }
                message WithEnum {
                  string name = 1;
                  Status status = 2;
                }
                """);

        final var statusField = struct.field("status");
        assertThat(statusField.schema()).isInstanceOf(EnumSchema.class);
        final var enumSchema = (EnumSchema) statusField.schema();
        assertThat(enumSchema.name()).isEqualTo("Status");
        assertThat(enumSchema.symbols()).hasSize(3);
        assertThat(enumSchema.symbols().stream().map(EnumSchema.Symbol::name))
                .containsExactly("ACTIVE", "INACTIVE", "PENDING");
    }

    // ===== toDataSchema: nested messages =====

    @Test
    @DisplayName("Nested message field maps to nested StructSchema with the inner message's fields")
    void toDataSchema_nestedMessage_mapsToNestedStructSchema() {
        final var struct = toStruct("Outer", """
                syntax = "proto3";
                package io.axual.ksml.test;
                message Inner {
                  string id = 1;
                }
                message Outer {
                  string name = 1;
                  Inner inner = 2;
                }
                """);

        final var innerField = struct.field("inner");
        assertThat(innerField.schema()).isInstanceOf(StructSchema.class);
        final var innerSchema = (StructSchema) innerField.schema();
        assertThat(innerSchema.name()).isEqualTo("Inner");
        assertThat(innerSchema.field("id").schema()).isEqualTo(DataSchema.STRING_SCHEMA);
    }

    // ===== toDataSchema: proto2 optionality =====

    @Test
    @DisplayName("Proto2 required label sets required=true; optional label sets required=false")
    void toDataSchema_proto2Labels_requiredFlagSetCorrectly() {
        final var struct = toStruct("Proto2Msg", """
                syntax = "proto2";
                package io.axual.ksml.test;
                message Proto2Msg {
                  required string mandatory = 1;
                  optional string optionalField = 2;
                }
                """);

        assertThat(struct.field("mandatory").required()).isTrue();
        assertThat(struct.field("optionalField").required()).isFalse();
    }

    @Test
    @DisplayName("Proto3 implicit fields (no label) are treated as not required")
    void toDataSchema_proto3ImplicitFields_notRequired() {
        final var struct = toStruct("Proto3Msg", """
                syntax = "proto3";
                package io.axual.ksml.test;
                message Proto3Msg {
                  string name = 1;
                }
                """);

        assertThat(struct.field("name").required()).isFalse();
    }

    @Test
    @DisplayName("Proto2 optional field with explicit default value maps to that default")
    void toDataSchema_proto2FieldWithDefault_mapsDefaultValueCorrectly() {
        final var struct = toStruct("WithDefault", """
                syntax = "proto2";
                package io.axual.ksml.test;
                message WithDefault {
                  optional string status = 1 [default = "active"];
                }
                """);

        final var field = struct.field("status");
        assertThat(field.defaultValue()).isEqualTo(new DataString("active"));
    }

    // ===== toDataSchema: error cases =====

    @Test
    @DisplayName("Message not found in ProtoFileElement throws SchemaException naming the missing type")
    void toDataSchema_messageNotFound_throwsSchemaException() {
        final var proto = """
                syntax = "proto3";
                package io.axual.ksml.test;
                message Other {
                  string name = 1;
                }
                """;
        final var fileElement = ProtoParser.Companion.parse(Location.get(""), proto);
        assertThatThrownBy(() -> mapper.toDataSchema(NS, "NotExisting", fileElement))
                .isInstanceOf(SchemaException.class)
                .hasMessageContaining("NotExisting");
    }

    // ===== fromDataSchema: round-trip =====

    @Test
    @DisplayName("StructSchema with primitive fields round-trips to ProtoFileElement with stable field types")
    void fromDataSchema_primitiveStruct_roundTripsCorrectly() {
        final var original = toStruct("Event", """
                syntax = "proto3";
                package io.axual.ksml.test;
                message Event {
                  string name = 1;
                  int32 count = 2;
                  boolean flag = 3;
                  double score = 4;
                }
                """);

        final var fileElement = mapper.fromDataSchema(original);
        assertThat(fileElement).isNotNull();
        assertThat(fileElement.getPackageName()).isEqualTo(NS);

        final var roundTripped = (StructSchema) mapper.toDataSchema(NS, "Event", fileElement);
        assertThat(roundTripped.field("name").schema()).isEqualTo(DataSchema.STRING_SCHEMA);
        assertThat(roundTripped.field("count").schema()).isEqualTo(DataSchema.INTEGER_SCHEMA);
        assertThat(roundTripped.field("flag").schema()).isEqualTo(DataSchema.BOOLEAN_SCHEMA);
        assertThat(roundTripped.field("score").schema()).isEqualTo(DataSchema.DOUBLE_SCHEMA);
    }

    @Test
    @DisplayName("StructSchema with enum round-trips with correct enum symbols")
    void fromDataSchema_withEnum_roundTrips() {
        final var original = toStruct("WithEnum", """
                syntax = "proto3";
                package io.axual.ksml.test;
                enum Color { RED = 0; GREEN = 1; BLUE = 2; }
                message WithEnum {
                  string name = 1;
                  Color color = 2;
                }
                """);

        final var fileElement = mapper.fromDataSchema(original);
        final var roundTripped = (StructSchema) mapper.toDataSchema(NS, "WithEnum", fileElement);

        assertThat(roundTripped.field("name").schema()).isEqualTo(DataSchema.STRING_SCHEMA);
        assertThat(roundTripped.field("color").schema()).isInstanceOf(EnumSchema.class);
        final var colorEnum = (EnumSchema) roundTripped.field("color").schema();
        assertThat(colorEnum.symbols()).hasSize(3);
        assertThat(colorEnum.symbols().stream().map(EnumSchema.Symbol::name))
                .containsExactly("RED", "GREEN", "BLUE");
    }

    @Test
    @DisplayName("StructSchema with repeated field round-trips as a ListSchema")
    void fromDataSchema_withRepeatedField_roundTrips() {
        final var original = toStruct("WithList", """
                syntax = "proto3";
                package io.axual.ksml.test;
                message WithList {
                  repeated string tags = 1;
                }
                """);

        final var fileElement = mapper.fromDataSchema(original);
        final var roundTripped = (StructSchema) mapper.toDataSchema(NS, "WithList", fileElement);

        assertThat(roundTripped.field("tags").schema()).isInstanceOf(ListSchema.class);
        assertThat(((ListSchema) roundTripped.field("tags").schema()).valueSchema()).isEqualTo(DataSchema.STRING_SCHEMA);
    }

    @Test
    @DisplayName("StructSchema with nested message round-trips with correctly structured nested schema")
    void fromDataSchema_withNestedMessage_roundTrips() {
        final var original = toStruct("Outer", """
                syntax = "proto3";
                package io.axual.ksml.test;
                message Inner {
                  int32 value = 1;
                }
                message Outer {
                  string name = 1;
                  Inner inner = 2;
                }
                """);

        final var fileElement = mapper.fromDataSchema(original);
        final var roundTripped = (StructSchema) mapper.toDataSchema(NS, "Outer", fileElement);

        assertThat(roundTripped.field("name").schema()).isEqualTo(DataSchema.STRING_SCHEMA);
        assertThat(roundTripped.field("inner").schema()).isInstanceOf(StructSchema.class);
        final var innerSchema = (StructSchema) roundTripped.field("inner").schema();
        assertThat(innerSchema.field("value").schema()).isEqualTo(DataSchema.INTEGER_SCHEMA);
    }

    // ===== fromDataSchema: error cases =====

    @Test
    @DisplayName("Non-StructSchema passed to fromDataSchema throws SchemaException")
    void fromDataSchema_nonStructSchema_throwsSchemaException() {
        assertThatThrownBy(() -> mapper.fromDataSchema(DataSchema.STRING_SCHEMA))
                .isInstanceOf(SchemaException.class);
    }

    // ===== fromDataSchema: label edge cases =====

    @Test
    @DisplayName("Required field (required=true) produces a FieldElement with null label (proto2 required semantic)")
    void fromDataSchema_requiredField_producesNullLabel() {
        final var struct = StructSchema.builder()
                .namespace(NS).name("Req")
                .field(new StructSchema.Field("mandatory", DataSchema.STRING_SCHEMA, "", 1, true))
                .field(new StructSchema.Field("optionalField", DataSchema.INTEGER_SCHEMA, "", 2, false))
                .additionalFieldsAllowed(false)
                .build();

        final var fileElement = mapper.fromDataSchema(struct);

        final var msgElement = fileElement.getTypes().stream()
                .filter(t -> t instanceof MessageElement && "Req".equals(t.getName()))
                .map(t -> (MessageElement) t)
                .findFirst().orElseThrow();

        final var mandatoryField = msgElement.getFields().stream()
                .filter(f -> "mandatory".equals(f.getName())).findFirst().orElseThrow();
        assertThat(mandatoryField.getLabel()).isNull();

        final var optionalField = msgElement.getFields().stream()
                .filter(f -> "optionalField".equals(f.getName())).findFirst().orElseThrow();
        assertThat(optionalField.getLabel()).isEqualTo(Field.Label.OPTIONAL);
    }

    @Test
    @DisplayName("Field with a non-null non-DataNull default value produces FieldElement with that default as a string")
    void fromDataSchema_fieldWithNonNullDefault_includesDefaultInFieldElement() {
        final var struct = StructSchema.builder()
                .namespace(NS).name("WithDefault")
                .field(new StructSchema.Field("greeting", DataSchema.STRING_SCHEMA, "", 1, false, false, new DataString("hello")))
                .additionalFieldsAllowed(false)
                .build();

        final var fileElement = mapper.fromDataSchema(struct);

        final var msgElement = fileElement.getTypes().stream()
                .filter(t -> t instanceof MessageElement && "WithDefault".equals(t.getName()))
                .map(t -> (MessageElement) t)
                .findFirst().orElseThrow();
        final var greetingField = msgElement.getFields().stream()
                .filter(f -> "greeting".equals(f.getName())).findFirst().orElseThrow();
        assertThat(greetingField.getDefaultValue()).isEqualTo("hello");
    }

    // ===== fromDataSchema: MapSchema is silently dropped =====

    @Test
    @DisplayName("MapSchema field is dropped from the output (proto does not support Avro-style maps via this path)")
    void fromDataSchema_mapSchemaField_silentlyDropped() {
        // Intended behavior: convertDataSchemaToProtoType returns null for a MapSchema, and
        // convertStructSchemaToMessageElement skips fields with a null proto type. This test pins
        // that contract so a future change to map handling is a conscious decision, not a surprise.
        final var struct = StructSchema.builder()
                .namespace(NS).name("WithMap")
                .field(new StructSchema.Field("kept", DataSchema.STRING_SCHEMA, "", 1))
                .field(new StructSchema.Field("dropped", new MapSchema(DataSchema.STRING_SCHEMA), "", 2))
                .additionalFieldsAllowed(false)
                .build();

        final var fileElement = mapper.fromDataSchema(struct);

        final var msgElement = fileElement.getTypes().stream()
                .filter(t -> t instanceof MessageElement && "WithMap".equals(t.getName()))
                .map(t -> (MessageElement) t)
                .findFirst().orElseThrow();
        assertThat(msgElement.getFields()).hasSize(1);
        assertThat(msgElement.getFields().getFirst().getName()).isEqualTo("kept");
    }

    // ===== fromDataSchema: nested type deduplication (ProtobufWriteContext.notDuplicate) =====

    @Test
    @DisplayName("Enum whose namespace matches parent-message namespace is emitted as a nested type inside that message")
    void fromDataSchema_enumWithParentNamespace_emittedAsNestedType() {
        // Enum namespace = NS + ".Container" triggers the nested-type path in convertDataSchemaToProtoType
        final var nestedEnum = new EnumSchema(NS + ".Container", "Status", null,
                List.of(new EnumSchema.Symbol("ACTIVE"), new EnumSchema.Symbol("INACTIVE")), null);
        final var struct = StructSchema.builder()
                .namespace(NS).name("Container")
                .field(new StructSchema.Field("status", nestedEnum, "", 1))
                .additionalFieldsAllowed(false)
                .build();

        final var fileElement = mapper.fromDataSchema(struct);

        final var msgElement = fileElement.getTypes().stream()
                .filter(t -> t instanceof MessageElement && "Container".equals(t.getName()))
                .map(t -> (MessageElement) t)
                .findFirst().orElseThrow();
        assertThat(msgElement.getNestedTypes()).hasSize(1);
        assertThat(msgElement.getNestedTypes().getFirst()).isInstanceOf(EnumElement.class);
        assertThat(msgElement.getNestedTypes().getFirst().getName()).isEqualTo("Status");
    }

    @Test
    @DisplayName("Same nested enum referenced twice is emitted only once (notDuplicate returns false on second call)")
    void fromDataSchema_sameNestedEnumReferencedTwice_deduplicatedToOneNestedType() {
        final var nestedEnum = new EnumSchema(NS + ".Container", "Status", null,
                List.of(new EnumSchema.Symbol("ACTIVE"), new EnumSchema.Symbol("INACTIVE")), null);
        final var struct = StructSchema.builder()
                .namespace(NS).name("Container")
                .field(new StructSchema.Field("status1", nestedEnum, "", 1))
                .field(new StructSchema.Field("status2", nestedEnum, "", 2))
                .additionalFieldsAllowed(false)
                .build();

        final var fileElement = mapper.fromDataSchema(struct);

        final var msgElement = fileElement.getTypes().stream()
                .filter(t -> t instanceof MessageElement && "Container".equals(t.getName()))
                .map(t -> (MessageElement) t)
                .findFirst().orElseThrow();
        assertThat(msgElement.getNestedTypes()).hasSize(1);
    }

    @Test
    @DisplayName("StructSchema whose namespace matches parent-message namespace is emitted as a nested type")
    void fromDataSchema_structWithParentNamespace_emittedAsNestedType() {
        // Inner.doc must be non-null so Wire's FieldElement doesn't NPE on the inner field's doc
        final var innerStruct = new StructSchema(NS + ".Outer", "Inner", null,
                List.of(new StructSchema.Field("id", DataSchema.STRING_SCHEMA, "", 1)), false);
        final var outer = StructSchema.builder()
                .namespace(NS).name("Outer")
                .field(new StructSchema.Field("inner", innerStruct, "", 1))
                .additionalFieldsAllowed(false)
                .build();

        final var fileElement = mapper.fromDataSchema(outer);

        final var outerMsg = fileElement.getTypes().stream()
                .filter(t -> t instanceof MessageElement && "Outer".equals(t.getName()))
                .map(t -> (MessageElement) t)
                .findFirst().orElseThrow();
        assertThat(outerMsg.getNestedTypes()).hasSize(1);
        assertThat(outerMsg.getNestedTypes().getFirst()).isInstanceOf(MessageElement.class);
        assertThat(outerMsg.getNestedTypes().getFirst().getName()).isEqualTo("Inner");
    }
}
