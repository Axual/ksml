package io.axual.ksml.data.notation.protobuf;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - PROTOBUF
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
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
import com.squareup.wire.schema.internal.parser.*;
import io.apicurio.registry.utils.protobuf.schema.DynamicSchema;
import io.apicurio.registry.utils.protobuf.schema.EnumDefinition;
import io.apicurio.registry.utils.protobuf.schema.MessageDefinition;
import io.axual.ksml.data.exception.SchemaException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static io.axual.ksml.data.notation.protobuf.ProtobufConstants.*;

public class ApicurioProtobufDescriptorFileElementMapper implements ProtobufDescriptorFileElementMapper {
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
                if (type instanceof MessageElement messageElement) {
                    if (notDuplicateType(namespace, messageElement)) {
                        result.addMessageDefinition(toMessageDefinition(namespace, messageElement));
                    }
                }
                if (type instanceof EnumElement enumElement) {
                    if (notDuplicateType(namespace, enumElement)) {
                        result.addEnumDefinition(toEnumDefinition(namespace, enumElement));
                    }
                }
            }

            try {
                final var ds = result.build();
                return Descriptors.FileDescriptor.buildFrom(ds.getFileDescriptorProto(), ProtobufConstants.NO_DEPENDENCIES);
            } catch (Descriptors.DescriptorValidationException e) {
                throw new SchemaException("Can not generate dynamic PROTOBUF schema" + name + ": " + e.getMessage(), e);
            }
        }

        private MessageDefinition toMessageDefinition(String namespace, MessageElement messageElement) {
            // Mark the namespace + message name as done
            final var fullName = (namespace != null && !namespace.isEmpty() ? namespace + "." : "") + messageElement.getName();
            types.add(fullName);

            // Convert nested types
            final var msgBuilder = MessageDefinition.newBuilder(messageElement.getName());
            for (final var nestedType : messageElement.getNestedTypes()) {
                if (notDuplicateType(fullName, nestedType)) {
                    if (nestedType instanceof MessageElement nestedMessage) {
                        msgBuilder.addMessageDefinition(toMessageDefinition(fullName, nestedMessage));
                    }
                    if (nestedType instanceof EnumElement nestedEnum) {
                        msgBuilder.addEnumDefinition(toEnumDefinition(fullName, nestedEnum));
                    }
                }
            }

            // Convert oneOfs
            for (final var oneOf : messageElement.getOneOfs()) {
                final var oneOfBuilder = msgBuilder.addOneof(oneOf.getName());
                for (final var oneOfField : oneOf.getFields()) {
                    oneOfBuilder.addField(oneOfField.getType(), oneOfField.getName(), oneOfField.getTag(), null);
                }
            }

            // Convert fields
            for (final var field : messageElement.getFields()) {
                final var required = field.getLabel() == null || field.getLabel() == Field.Label.REQUIRED;
                final var repeated = field.getLabel() == Field.Label.REPEATED;
                final var label = required ? null : repeated ? "repeated" : "optional";
                msgBuilder.addField(label, field.getType(), field.getName(), field.getTag(), null);
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

    private static class DescriptorToFileElementConverter {
        public ProtoFileElement convert(Descriptors.Descriptor descriptor) {
            final var types = new ArrayList<TypeElement>();
            for (final var msg : descriptor.getFile().getMessageTypes()) {
                types.add(convertToMessageElement(msg));
            }
            for (final var enm : descriptor.getFile().getEnumTypes()) {
                types.add(convertToEnumElement(enm));
            }
            return new ProtoFileElement(
                    DEFAULT_LOCATION,
                    descriptor.getFile().getPackage(),
                    DEFAULT_SYNTAX,
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    types,
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyList());
        }

        private static String defaultValue(Descriptors.FieldDescriptor field) {
            if (field.getType() == Descriptors.FieldDescriptor.Type.MESSAGE) return null;
            final var defaultValue = field.getDefaultValue();
            return defaultValue != null ? defaultValue.toString() : null;
        }

        private MessageElement convertToMessageElement(Descriptors.Descriptor messageDescriptor) {
            // Convert the oneOfs to OneOfElements
            final var oneOfs = new ArrayList<OneOfElement>();
            for (final var oneOf : messageDescriptor.getOneofs()) {
                final var oneOfFields = new ArrayList<FieldElement>();
                for (final var oneOfField : oneOf.getFields()) {
                    final var label = oneOfField.isRequired() ? Field.Label.REQUIRED : oneOfField.isRepeated() ? Field.Label.REPEATED : Field.Label.OPTIONAL;
                    final var type = convertType(oneOfField);
                    oneOfFields.add(new FieldElement(DEFAULT_LOCATION, label, type, oneOfField.getName(), defaultValue(oneOfField), null, oneOfField.getNumber(), NO_DOCUMENTATION, Collections.emptyList()));
                }
                oneOfs.add(new OneOfElement(oneOf.getName(), NO_DOCUMENTATION, oneOfFields, Collections.emptyList(), Collections.emptyList(), DEFAULT_LOCATION));
            }

            // Convert nested messages
            final var nestedTypes = new ArrayList<TypeElement>();
            for (final var nestedType : messageDescriptor.getNestedTypes()) {
                nestedTypes.add(convertToMessageElement(nestedType));
            }

            // Convert nested enums
            for (final var enumType : messageDescriptor.getEnumTypes()) {
                nestedTypes.add(convertToEnumElement(enumType));
            }

            // Convert the message fields
            final var fields = new ArrayList<FieldElement>();
            for (final var field : messageDescriptor.getFields()) {
                if (field.getContainingOneof() == null) {
                    final var type = convertType(field);
                    if (type != null) fields.add(convertToFieldElement(field, type));
                }
            }

            return new MessageElement(
                    DEFAULT_LOCATION,
                    messageDescriptor.getName(),
                    NO_DOCUMENTATION,
                    nestedTypes,
                    Collections.emptyList(),
                    Collections.emptyList(),
                    fields,
                    oneOfs,
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyList());
        }

        private static FieldElement convertToFieldElement(Descriptors.FieldDescriptor field, String type) {
            final var required = field.isRequired();
            final var list = field.isRepeated();
            return new FieldElement(
                    DEFAULT_LOCATION,
                    required ? null : list ? Field.Label.REPEATED : Field.Label.OPTIONAL,
                    type,
                    field.getName(),
                    defaultValue(field),
                    null,
                    field.getNumber(),
                    NO_DOCUMENTATION,
                    Collections.emptyList());
        }

        private EnumElement convertToEnumElement(Descriptors.EnumDescriptor enumDescriptor) {
            final var constants = enumDescriptor.getValues().stream().map(symbol -> new EnumConstantElement(DEFAULT_LOCATION, symbol.getName(), symbol.getIndex(), NO_DOCUMENTATION, Collections.emptyList())).toList();
            return new EnumElement(DEFAULT_LOCATION, enumDescriptor.getName(), NO_DOCUMENTATION, Collections.emptyList(), constants, Collections.emptyList());
        }

        private String convertType(Descriptors.FieldDescriptor field) {
            return switch (field.getType()) {
                case DOUBLE, FLOAT, INT64, UINT64, INT32, FIXED64, FIXED32, BOOL, STRING, BYTES, UINT32, SFIXED32,
                     SFIXED64, SINT32, SINT64 -> field.getType().toString().toLowerCase();
                default -> field.toProto().getTypeName();
            };
        }
    }
}
