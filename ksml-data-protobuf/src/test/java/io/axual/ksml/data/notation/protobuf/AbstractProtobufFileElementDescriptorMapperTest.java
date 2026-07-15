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

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.squareup.wire.schema.Field;
import com.squareup.wire.schema.Location;
import com.squareup.wire.schema.internal.parser.EnumElement;
import com.squareup.wire.schema.internal.parser.MessageElement;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import com.squareup.wire.schema.internal.parser.ProtoParser;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Shared behavior for the per-vendor {@code ProtobufFileElementDescriptorMapper} tests.
 *
 * <p>The Apicurio and Confluent mappers convert between the same two representations
 * ({@link ProtoFileElement} and protobuf {@link Descriptors.FileDescriptor}) and must behave
 * identically for every case except how a {@code oneof} group is exposed on the built descriptor,
 * which differs because Confluent goes through {@code DynamicSchema}. Subclasses supply the mapper
 * via {@link #toDescriptor} / {@link #toFileElement} and the vendor-specific oneof assertion via
 * {@link #assertOneOfBranches}.
 */
abstract class AbstractProtobufFileElementDescriptorMapperTest {

    protected static final String NS = "io.axual.ksml.test";

    /** Convert a parsed proto file to a protobuf FileDescriptor using the vendor mapper under test. */
    protected abstract Descriptors.FileDescriptor toDescriptor(String namespace, String fileName, ProtoFileElement fileElement);

    /** Convert a protobuf message descriptor back to a wire ProtoFileElement using the vendor mapper under test. */
    protected abstract ProtoFileElement toFileElement(Descriptors.Descriptor descriptor);

    /** Assert the branches of the {@code payload} oneof on a built descriptor (exposed differently per vendor). */
    protected abstract void assertOneOfBranches(Descriptors.Descriptor msgDescriptor);

    private static ProtoFileElement parseProto(String proto) {
        return ProtoParser.Companion.parse(Location.get(""), proto);
    }

    // ===== toDescriptor =====

    @Test
    @DisplayName("Simple message with primitive fields produces FileDescriptor with correct field types")
    void toDescriptor_simpleMessage_buildsCorrectFileDescriptor() {
        final var fileElement = parseProto("""
                syntax = "proto3";
                package io.axual.ksml.test;
                message Simple {
                  string name = 1;
                  int32 count = 2;
                }
                """);

        final var fileDescriptor = toDescriptor(NS, "simple.proto", fileElement);

        assertThat(fileDescriptor).isNotNull();
        final var msgDescriptor = fileDescriptor.findMessageTypeByName("Simple");
        assertThat(msgDescriptor).isNotNull();
        assertThat(msgDescriptor.findFieldByName("name")).isNotNull();
        assertThat(msgDescriptor.findFieldByName("count")).isNotNull();
        assertThat(msgDescriptor.findFieldByName("name").getType()).isEqualTo(Descriptors.FieldDescriptor.Type.STRING);
        assertThat(msgDescriptor.findFieldByName("count").getType()).isEqualTo(Descriptors.FieldDescriptor.Type.INT32);
    }

    @Test
    @DisplayName("Message with top-level enum produces FileDescriptor with enum values accessible from the field")
    void toDescriptor_withEnum_buildsFileDescriptorWithEnum() {
        final var fileElement = parseProto("""
                syntax = "proto3";
                package io.axual.ksml.test;
                enum Status { ACTIVE = 0; INACTIVE = 1; }
                message WithEnum {
                  string name = 1;
                  Status status = 2;
                }
                """);

        final var fileDescriptor = toDescriptor(NS, "with_enum.proto", fileElement);

        assertThat(fileDescriptor).isNotNull();
        final var msgDescriptor = fileDescriptor.findMessageTypeByName("WithEnum");
        assertThat(msgDescriptor).isNotNull();
        final var statusField = msgDescriptor.findFieldByName("status");
        assertThat(statusField).isNotNull();
        assertThat(statusField.getType()).isEqualTo(Descriptors.FieldDescriptor.Type.ENUM);
        assertThat(statusField.getEnumType().getValues()).hasSize(2);
        assertThat(statusField.getEnumType().findValueByName("ACTIVE")).isNotNull();
        assertThat(statusField.getEnumType().findValueByName("INACTIVE")).isNotNull();
    }

    @Test
    @DisplayName("Message with oneOf block produces FileDescriptor with the expected oneOf branches")
    void toDescriptor_withOneOf_buildsFileDescriptorWithOneOf() {
        final var fileElement = parseProto("""
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

        final var fileDescriptor = toDescriptor(NS, "with_oneof.proto", fileElement);

        assertThat(fileDescriptor).isNotNull();
        final var msgDescriptor = fileDescriptor.findMessageTypeByName("WithOneOf");
        assertThat(msgDescriptor).isNotNull();
        assertOneOfBranches(msgDescriptor);
    }

    @Test
    @DisplayName("Outer message referencing another message produces FileDescriptor with MESSAGE-typed field")
    void toDescriptor_withNestedMessage_buildsFileDescriptorWithMessageField() {
        final var fileElement = parseProto("""
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

        final var fileDescriptor = toDescriptor(NS, "nested.proto", fileElement);

        assertThat(fileDescriptor).isNotNull();
        final var outerMsg = fileDescriptor.findMessageTypeByName("Outer");
        assertThat(outerMsg).isNotNull();
        final var innerField = outerMsg.findFieldByName("inner");
        assertThat(innerField).isNotNull();
        assertThat(innerField.getType()).isEqualTo(Descriptors.FieldDescriptor.Type.MESSAGE);
        assertThat(innerField.getMessageType().getName()).isEqualTo("Inner");
    }

    @Test
    @DisplayName("Repeated references to the same message type do not cause duplicate-type errors")
    void toDescriptor_duplicateMessageTypeReferences_deduplicatedSuccessfully() {
        final var fileElement = parseProto("""
                syntax = "proto3";
                package io.axual.ksml.test;
                message Inner {
                  string id = 1;
                }
                message Outer {
                  Inner first = 1;
                  Inner second = 2;
                }
                """);

        final var fileDescriptor = toDescriptor(NS, "dedup.proto", fileElement);

        assertThat(fileDescriptor).isNotNull();
        assertThat(fileDescriptor.findMessageTypeByName("Inner")).isNotNull();
        assertThat(fileDescriptor.findMessageTypeByName("Outer")).isNotNull();
    }

    @Test
    @DisplayName("Message with repeated (list) field produces FileDescriptor with REPEATED-label field")
    void toDescriptor_withRepeatedField_buildsFileDescriptorWithRepeatedField() {
        final var fileElement = parseProto("""
                syntax = "proto3";
                package io.axual.ksml.test;
                message WithList {
                  repeated string tags = 1;
                }
                """);

        final var fileDescriptor = toDescriptor(NS, "list.proto", fileElement);

        assertThat(fileDescriptor).isNotNull();
        final var msgDescriptor = fileDescriptor.findMessageTypeByName("WithList");
        assertThat(msgDescriptor).isNotNull();
        final var tagsField = msgDescriptor.findFieldByName("tags");
        assertThat(tagsField).isNotNull();
        assertThat(tagsField.isRepeated()).isTrue();
        assertThat(tagsField.getType()).isEqualTo(Descriptors.FieldDescriptor.Type.STRING);
    }

    // ===== toFileElement =====

    @Test
    @DisplayName("Simple descriptor converts to ProtoFileElement with expected message and fields")
    void toFileElement_simpleDescriptor_producesCorrectProtoFileElement() throws Descriptors.DescriptorValidationException {
        final var msgDescriptor = buildSimpleMessageDescriptor();

        final var fileElement = toFileElement(msgDescriptor);

        assertThat(fileElement).isNotNull();
        assertThat(fileElement.getPackageName()).isEqualTo(NS);
        final var msgTypes = fileElement.getTypes().stream()
                .filter(MessageElement.class::isInstance)
                .map(t -> (MessageElement) t)
                .toList();
        assertThat(msgTypes).hasSize(1);
        final var msgElement = msgTypes.getFirst();
        assertThat(msgElement.getName()).isEqualTo("Simple");

        final var fieldNames = msgElement.getFields().stream().map(f -> f.getName()).toList();
        assertThat(fieldNames).containsExactlyInAnyOrder("name", "count");
        final var nameField = msgElement.getFields().stream().filter(f -> "name".equals(f.getName())).findFirst();
        assertThat(nameField).isPresent();
        assertThat(nameField.get().getType()).isEqualTo("string");
    }

    @Test
    @DisplayName("Descriptor with top-level enum type produces ProtoFileElement with enum type entry")
    void toFileElement_withEnum_producesProtoFileElementWithEnumType() throws Descriptors.DescriptorValidationException {
        final var msgDescriptor = buildDescriptorWithEnum();

        final var fileElement = toFileElement(msgDescriptor);

        assertThat(fileElement).isNotNull();
        final var enumTypes = fileElement.getTypes().stream()
                .filter(EnumElement.class::isInstance)
                .map(t -> (EnumElement) t)
                .toList();
        assertThat(enumTypes).isNotEmpty();
        final var statusEnum = enumTypes.stream().filter(e -> "Status".equals(e.getName())).findFirst();
        assertThat(statusEnum).isPresent();
        assertThat(statusEnum.get().getConstants()).hasSize(2);
    }

    @Test
    @DisplayName("Descriptor with oneOf produces ProtoFileElement with oneOf element and correct branches")
    void toFileElement_withOneOf_producesProtoFileElementWithOneOf() throws Descriptors.DescriptorValidationException {
        final var msgDescriptor = buildDescriptorWithOneOf();

        final var fileElement = toFileElement(msgDescriptor);

        assertThat(fileElement).isNotNull();
        final var msgTypes = fileElement.getTypes().stream()
                .filter(MessageElement.class::isInstance)
                .map(t -> (MessageElement) t)
                .toList();
        assertThat(msgTypes).isNotEmpty();
        final var withOneOf = msgTypes.stream().filter(m -> "WithOneOf".equals(m.getName())).findFirst();
        assertThat(withOneOf).isPresent();
        assertThat(withOneOf.get().getOneOfs()).hasSize(1);
        assertThat(withOneOf.get().getOneOfs().getFirst().getName()).isEqualTo("payload");
        assertThat(withOneOf.get().getOneOfs().getFirst().getFields()).hasSize(2);
    }

    @Test
    @DisplayName("Descriptor with nested message produces ProtoFileElement with both message types")
    void toFileElement_withNestedMessage_producesProtoFileElementWithBothMessageTypes() throws Descriptors.DescriptorValidationException {
        final var msgDescriptor = buildDescriptorWithNestedMessage();

        final var fileElement = toFileElement(msgDescriptor);

        assertThat(fileElement).isNotNull();
        final var msgTypes = fileElement.getTypes().stream()
                .filter(MessageElement.class::isInstance)
                .map(t -> (MessageElement) t)
                .toList();
        final var names = msgTypes.stream().map(MessageElement::getName).toList();
        assertThat(names).contains("Outer", "Inner");
    }

    // ===== nested enum inside message =====

    @Test
    @DisplayName("Message with enum defined inside it (nested) produces FileDescriptor with enum accessible via field")
    void toDescriptor_withNestedEnumInsideMessage_producesDescriptorWithEnumField() {
        final var fileElement = parseProto("""
                syntax = "proto3";
                package io.axual.ksml.test;
                message WithNestedEnum {
                  enum Status { ACTIVE = 0; INACTIVE = 1; }
                  Status status = 1;
                  string name = 2;
                }
                """);

        final var fileDescriptor = toDescriptor(NS, "nested_enum.proto", fileElement);

        assertThat(fileDescriptor).isNotNull();
        final var msgDescriptor = fileDescriptor.findMessageTypeByName("WithNestedEnum");
        assertThat(msgDescriptor).isNotNull();
        final var statusField = msgDescriptor.findFieldByName("status");
        assertThat(statusField).isNotNull();
        assertThat(statusField.getType()).isEqualTo(Descriptors.FieldDescriptor.Type.ENUM);
        assertThat(statusField.getEnumType().findValueByName("ACTIVE")).isNotNull();
        assertThat(statusField.getEnumType().findValueByName("INACTIVE")).isNotNull();
    }

    @Test
    @DisplayName("Descriptor with enum nested inside a message produces ProtoFileElement with enum in message nested types")
    void toFileElement_withNestedEnumInsideMessage_producesNestedEnumInMessage() throws Descriptors.DescriptorValidationException {
        final var msgDescriptor = buildDescriptorWithNestedEnum();

        final var fileElement = toFileElement(msgDescriptor);

        assertThat(fileElement).isNotNull();
        final var withNestedEnum = fileElement.getTypes().stream()
                .filter(t -> t instanceof MessageElement && "WithNestedEnum".equals(t.getName()))
                .map(t -> (MessageElement) t)
                .findFirst().orElseThrow();
        final var nestedEnums = withNestedEnum.getNestedTypes().stream()
                .filter(EnumElement.class::isInstance)
                .toList();
        assertThat(nestedEnums).hasSize(1);
        assertThat(nestedEnums.getFirst().getName()).isEqualTo("Status");
    }

    @Test
    @DisplayName("Descriptor with a repeated (list) field produces ProtoFileElement with REPEATED label on that field")
    void toFileElement_withRepeatedField_producesRepeatedLabelOnField() throws Descriptors.DescriptorValidationException {
        final var msgDescriptor = buildDescriptorWithRepeatedField();

        final var fileElement = toFileElement(msgDescriptor);

        assertThat(fileElement).isNotNull();
        final var msgTypes = fileElement.getTypes().stream()
                .filter(MessageElement.class::isInstance)
                .map(t -> (MessageElement) t)
                .toList();
        assertThat(msgTypes).hasSize(1);
        final var tagsField = msgTypes.getFirst().getFields().stream()
                .filter(f -> "tags".equals(f.getName())).findFirst().orElseThrow();
        assertThat(tagsField.getLabel()).isEqualTo(Field.Label.REPEATED);
        assertThat(tagsField.getType()).isEqualTo("string");
    }

    // ===== descriptor builder helpers =====

    private Descriptors.Descriptor buildSimpleMessageDescriptor() throws Descriptors.DescriptorValidationException {
        final var nameField = DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName("name").setNumber(1)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .build();
        final var countField = DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName("count").setNumber(2)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .build();
        final var msgProto = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Simple")
                .addField(nameField)
                .addField(countField)
                .build();
        final var fileProto = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("simple.proto").setSyntax("proto3")
                .setPackage(NS)
                .addMessageType(msgProto)
                .build();
        final var fileDescriptor = Descriptors.FileDescriptor.buildFrom(fileProto, ProtobufConstants.NO_DEPENDENCIES);
        return fileDescriptor.findMessageTypeByName("Simple");
    }

    private Descriptors.Descriptor buildDescriptorWithEnum() throws Descriptors.DescriptorValidationException {
        final var statusEnum = DescriptorProtos.EnumDescriptorProto.newBuilder()
                .setName("Status")
                .addValue(DescriptorProtos.EnumValueDescriptorProto.newBuilder().setName("ACTIVE").setNumber(0).build())
                .addValue(DescriptorProtos.EnumValueDescriptorProto.newBuilder().setName("INACTIVE").setNumber(1).build())
                .build();
        final var nameField = DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName("name").setNumber(1)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .build();
        final var statusField = DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName("status").setNumber(2)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_ENUM)
                .setTypeName("." + NS + ".Status")
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .build();
        final var msgProto = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("WithEnum")
                .addField(nameField)
                .addField(statusField)
                .build();
        final var fileProto = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("with_enum.proto").setSyntax("proto3")
                .setPackage(NS)
                .addEnumType(statusEnum)
                .addMessageType(msgProto)
                .build();
        final var fileDescriptor = Descriptors.FileDescriptor.buildFrom(fileProto, ProtobufConstants.NO_DEPENDENCIES);
        return fileDescriptor.findMessageTypeByName("WithEnum");
    }

    private Descriptors.Descriptor buildDescriptorWithOneOf() throws Descriptors.DescriptorValidationException {
        final var oneof = DescriptorProtos.OneofDescriptorProto.newBuilder().setName("payload").build();
        final var textField = DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName("text").setNumber(1).setOneofIndex(0)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .build();
        final var countField = DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName("count").setNumber(2).setOneofIndex(0)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .build();
        final var msgProto = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("WithOneOf")
                .addOneofDecl(oneof)
                .addField(textField)
                .addField(countField)
                .build();
        final var fileProto = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("with_oneof.proto").setSyntax("proto3")
                .setPackage(NS)
                .addMessageType(msgProto)
                .build();
        final var fileDescriptor = Descriptors.FileDescriptor.buildFrom(fileProto, ProtobufConstants.NO_DEPENDENCIES);
        return fileDescriptor.findMessageTypeByName("WithOneOf");
    }

    private Descriptors.Descriptor buildDescriptorWithNestedEnum() throws Descriptors.DescriptorValidationException {
        final var statusEnum = DescriptorProtos.EnumDescriptorProto.newBuilder()
                .setName("Status")
                .addValue(DescriptorProtos.EnumValueDescriptorProto.newBuilder().setName("ACTIVE").setNumber(0).build())
                .addValue(DescriptorProtos.EnumValueDescriptorProto.newBuilder().setName("INACTIVE").setNumber(1).build())
                .build();
        final var nameField = DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName("name").setNumber(1)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .build();
        final var statusField = DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName("status").setNumber(2)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_ENUM)
                .setTypeName("." + NS + ".WithNestedEnum.Status")
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .build();
        final var msgProto = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("WithNestedEnum")
                .addEnumType(statusEnum)
                .addField(nameField)
                .addField(statusField)
                .build();
        final var fileProto = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("nested_enum.proto").setSyntax("proto3")
                .setPackage(NS)
                .addMessageType(msgProto)
                .build();
        final var fileDescriptor = Descriptors.FileDescriptor.buildFrom(fileProto, ProtobufConstants.NO_DEPENDENCIES);
        return fileDescriptor.findMessageTypeByName("WithNestedEnum");
    }

    private Descriptors.Descriptor buildDescriptorWithRepeatedField() throws Descriptors.DescriptorValidationException {
        final var tagsField = DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName("tags").setNumber(1)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED)
                .build();
        final var msgProto = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("WithRepeated")
                .addField(tagsField)
                .build();
        final var fileProto = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("repeated.proto").setSyntax("proto3")
                .setPackage(NS)
                .addMessageType(msgProto)
                .build();
        final var fileDescriptor = Descriptors.FileDescriptor.buildFrom(fileProto, ProtobufConstants.NO_DEPENDENCIES);
        return fileDescriptor.findMessageTypeByName("WithRepeated");
    }

    private Descriptors.Descriptor buildDescriptorWithNestedMessage() throws Descriptors.DescriptorValidationException {
        final var innerIdField = DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName("id").setNumber(1)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .build();
        final var innerMsgProto = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Inner")
                .addField(innerIdField)
                .build();
        final var outerNameField = DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName("name").setNumber(1)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .build();
        final var outerInnerField = DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName("inner").setNumber(2)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                .setTypeName("." + NS + ".Inner")
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .build();
        final var outerMsgProto = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Outer")
                .addField(outerNameField)
                .addField(outerInnerField)
                .build();
        final var fileProto = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("nested.proto").setSyntax("proto3")
                .setPackage(NS)
                .addMessageType(innerMsgProto)
                .addMessageType(outerMsgProto)
                .build();
        final var fileDescriptor = Descriptors.FileDescriptor.buildFrom(fileProto, ProtobufConstants.NO_DEPENDENCIES);
        return fileDescriptor.findMessageTypeByName("Outer");
    }
}
