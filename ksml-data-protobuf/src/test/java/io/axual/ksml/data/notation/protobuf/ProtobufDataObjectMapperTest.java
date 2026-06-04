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
import com.google.protobuf.DynamicMessage;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.schema.UnionSchema;
import io.axual.ksml.data.type.DataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Tests for {@link ProtobufDataObjectMapper#setMessageFieldValue}.
 *
 * <p>The previous implementation called {@code msg.setField(field, value)} directly. When the value
 * did not match the field's protobuf type (e.g. a Java {@code Long} larger than INT32 sent to an
 * INT32 field) the failure came from deep inside the protobuf library as a generic
 * {@code ClassCastException} at message-build time. The new behaviour wraps such failures into a
 * {@link DataException} that names the offending field and value type.</p>
 */
class ProtobufDataObjectMapperTest {

    private Descriptors.Descriptor messageDescriptor;
    private ProtobufDataObjectMapper mapper;

    @BeforeEach
    void setUp() throws Descriptors.DescriptorValidationException {
        // Build a minimal protobuf descriptor with a single INT32 field, programmatically — no
        // dependency on the Apicurio/Confluent submodule descriptor mappers.
        final var fieldProto = DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName("n")
                .setNumber(1)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .build();
        final var messageProto = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("TinyMessage")
                .addField(fieldProto)
                .build();
        final var fileProto = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("tiny.proto")
                .setSyntax("proto3")
                .addMessageType(messageProto)
                .build();
        final var fileDescriptor = Descriptors.FileDescriptor.buildFrom(fileProto, new Descriptors.FileDescriptor[0]);
        messageDescriptor = fileDescriptor.findMessageTypeByName("TinyMessage");

        // The descriptorElementMapper is only used by convertDataStructToMessage; setMessageFieldValue
        // never touches it, so we pass null to keep the test minimal.
        mapper = new ProtobufDataObjectMapper(null);
    }

    @Test
    @DisplayName("setMessageFieldValue: in-range INT32 value is set successfully")
    void setMessageFieldValue_validValue_setsField() {
        final var builder = DynamicMessage.newBuilder(messageDescriptor);
        final var intField = messageDescriptor.findFieldByName("n");
        mapper.setMessageFieldValue(builder, intField, 42);
        assertThat(builder.build().getField(intField)).isEqualTo(42);
    }

    @Test
    @DisplayName("setMessageFieldValue: value of wrong Java type throws DataException naming the field and value type")
    void setMessageFieldValue_wrongType_throwsDataException() {
        final var builder = DynamicMessage.newBuilder(messageDescriptor);
        final var intField = messageDescriptor.findFieldByName("n");
        // protobuf INT32 expects a Java Integer; a Long fails with a generic ClassCastException
        // inside the library. We expect the wrap to translate that into a DataException with
        // pointed context.
        assertThatCode(() -> mapper.setMessageFieldValue(builder, intField, 5L))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("Failed to set Protobuf field")
                .hasMessageContaining(intField.getFullName())
                .hasMessageContaining("Long");
    }

    @Test
    @DisplayName("setMessageFieldValue: DataString value to INT32 field throws DataException")
    void setMessageFieldValue_stringToInt_throwsDataException() {
        final var builder = DynamicMessage.newBuilder(messageDescriptor);
        final var intField = messageDescriptor.findFieldByName("n");
        assertThatCode(() -> mapper.setMessageFieldValue(builder, intField, "not an int"))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("Failed to set Protobuf field")
                .hasMessageContaining("String");
    }

    @Test
    @DisplayName("oneOf: value matching no branch throws DataException naming the oneOf field")
    void oneOf_noMatchingBranch_throws() throws Descriptors.DescriptorValidationException {
        // Build a protobuf message with a oneOf containing { string s; int32 n; }.
        final var oneof = DescriptorProtos.OneofDescriptorProto.newBuilder().setName("u").build();
        final var stringField = DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName("s").setNumber(1).setOneofIndex(0)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .build();
        final var intField = DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName("n").setNumber(2).setOneofIndex(0)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .build();
        final var messageProto = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("WithOneOf")
                .addOneofDecl(oneof)
                .addField(stringField)
                .addField(intField)
                .build();
        final var fileProto = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("oneof_test.proto")
                .setSyntax("proto3")
                .addMessageType(messageProto)
                .build();
        final var fileDescriptor = Descriptors.FileDescriptor.buildFrom(fileProto, new Descriptors.FileDescriptor[0]);

        // Test-local descriptor mapper returning the hand-built FileDescriptor regardless of input.
        final var testDescriptorMapper = new ProtobufFileElementDescriptorMapper() {
            @Override
            public Descriptors.FileDescriptor toDescriptor(String namespace, String name, ProtoFileElement fileElement) {
                return fileDescriptor;
            }

            @Override
            public ProtoFileElement toFileElement(Descriptors.Descriptor descriptor) {
                return null;
            }
        };

        // KSML schema mirrors the proto: a UnionSchema field "u" with string and int branches.
        // (member docs must be non-null because the protobuf wire-schema writer requires them.)
        final var union = new UnionSchema(
                new UnionSchema.Member("s", DataSchema.STRING_SCHEMA, "", 1),
                new UnionSchema.Member("n", DataSchema.INTEGER_SCHEMA, "", 2));
        final var structSchema = new StructSchema("io.axual.test", "WithOneOf", "test",
                List.of(new StructSchema.Field("u", union)), false);
        final var struct = new DataStruct(structSchema);
        // A DataList value matches neither STRING nor INTEGER branch → throws.
        struct.put("u", new DataList(DataType.UNKNOWN));

        final var mapperWithOneOf = new ProtobufDataObjectMapper(testDescriptorMapper);
        assertThatCode(() -> mapperWithOneOf.fromDataObject(struct))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("does not match any branch")
                .hasMessageContaining("'u'");
    }
}
