package io.axual.ksml.data.notation.protobuf.confluent;

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

import com.google.protobuf.Descriptors;
import com.squareup.wire.schema.Field;
import com.squareup.wire.schema.internal.parser.EnumElement;
import com.squareup.wire.schema.internal.parser.MessageElement;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import com.squareup.wire.schema.internal.parser.TypeElement;
import io.axual.ksml.data.exception.SchemaException;
import io.axual.ksml.data.notation.protobuf.DescriptorToFileElementConverter;
import io.axual.ksml.data.notation.protobuf.ProtobufConstants;
import io.axual.ksml.data.notation.protobuf.ProtobufFileElementDescriptorMapper;
import io.confluent.kafka.schemaregistry.protobuf.diff.Context;
import io.confluent.kafka.schemaregistry.protobuf.dynamic.DynamicSchema;
import io.confluent.kafka.schemaregistry.protobuf.dynamic.EnumDefinition;
import io.confluent.kafka.schemaregistry.protobuf.dynamic.FieldDefinition;
import io.confluent.kafka.schemaregistry.protobuf.dynamic.MessageDefinition;

import java.util.HashSet;
import java.util.Set;

public class ConfluentProtobufFileElementDescriptorMapper implements ProtobufFileElementDescriptorMapper {
    public Descriptors.FileDescriptor toDescriptor(String namespace, String name, ProtoFileElement fileElement) {
        return new ElementToDescriptorConverter().convert(namespace, name, fileElement);
    }

    public ProtoFileElement toFileElement(Descriptors.Descriptor descriptor) {
        return new DescriptorToFileElementConverter().convert(descriptor);
    }

    private static class ElementToDescriptorConverter {
        private final Set<String> types = new HashSet<>();

        private boolean notDuplicateType(String namespace, TypeElement type) {
            final var fullName = (namespace != null && !namespace.isEmpty() ? namespace + "." : "") + type.getName();
            final var result = !types.contains(fullName);
            types.add(fullName);
            return result;
        }

        public Descriptors.FileDescriptor convert(String namespace, String name, ProtoFileElement fileElement) {
            final var result = DynamicSchema.newBuilder();
            result.setSyntax(fileElement.getSyntax() != null
                    ? fileElement.getSyntax().toString()
                    : ProtobufConstants.DEFAULT_SYNTAX.toString());
            result.setPackage(namespace);
            result.setName(name);

            // Convert all top-level messages and enums
            for (final var type : fileElement.getTypes()) {
                if (type instanceof MessageElement messageElement && notDuplicateType(namespace, messageElement)) {
                    result.addMessageDefinition(toMessageDefinition(namespace, messageElement));
                }
                if (type instanceof EnumElement enumElement && notDuplicateType(namespace, enumElement)) {
                    result.addEnumDefinition(toEnumDefinition(namespace, enumElement));
                }
            }

            try {
                final var ds = result.build();
                return Descriptors.FileDescriptor.buildFrom(ds.getFileDescriptorProto(), ProtobufConstants.NO_DEPENDENCIES);
            } catch (Descriptors.DescriptorValidationException e) {
                throw new SchemaException("Can not generate dynamic PROTOBUF schema" + name + ": " + e.getMessage(), e);
            }
        }

        @SuppressWarnings("java:S3358")
        private MessageDefinition toMessageDefinition(String namespace, MessageElement messageElement) {
            // Mark the namespace + message name as done
            final var fullName = (namespace != null && !namespace.isEmpty() ? namespace + "." : "") + messageElement.getName();
            types.add(fullName);

            // Convert nested types
            final var msgBuilder = MessageDefinition.newBuilder(messageElement.getName());
            for (final var nestedType : messageElement.getNestedTypes()) {
                if (!notDuplicateType(fullName, nestedType)) {
                    continue;
                }
                if (nestedType instanceof MessageElement nestedMessage){
                    msgBuilder.addMessageDefinition(toMessageDefinition(fullName, nestedMessage));
                }
                if (nestedType instanceof EnumElement nestedEnum) {
                    msgBuilder.addEnumDefinition(toEnumDefinition(fullName, nestedEnum));
                }
            }

            // Convert oneOfs
            for (final var oneOf : messageElement.getOneOfs()) {
                final var oneOfBuilder = msgBuilder.addOneof(oneOf.getName());
                for (final var oneOfField : oneOf.getFields()) {
                    final var fld = FieldDefinition.newBuilder(new Context(), oneOfField.getName(), oneOfField.getTag(), oneOfField.getType()).build();
                    oneOfBuilder.addField(fld);
                }
            }

            // Convert fields
            for (final var field : messageElement.getFields()) {
                final var required = field.getLabel() == null || field.getLabel() == Field.Label.REQUIRED;
                final var repeated = field.getLabel() == Field.Label.REPEATED;
                final var label = required ? null
                        : repeated ? "repeated" : "optional";
                final var fldBuilder = FieldDefinition.newBuilder(new Context(), field.getName(), field.getTag(), field.getType());
                msgBuilder.addField(label != null ? fldBuilder.setLabel(label).build() : fldBuilder.build());
            }

            // Return definition
            return msgBuilder.build();
        }

        private EnumDefinition toEnumDefinition(String namespace, EnumElement enumElement) {
            // Mark the namespace + enum name as done
            final var fullName = (namespace != null && !namespace.isEmpty() ? namespace + "." : "") + enumElement.getName();
            types.add(fullName);

            final var enumBuilder = EnumDefinition.newBuilder(enumElement.getName());
            for (final var constant : enumElement.getConstants()) {
                enumBuilder.addValue(constant.getName(), constant.getTag());
            }
            return enumBuilder.build();
        }
    }
}
