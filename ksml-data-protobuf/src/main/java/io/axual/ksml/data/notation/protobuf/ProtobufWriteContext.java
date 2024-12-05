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
import com.squareup.wire.Syntax;
import com.squareup.wire.schema.Field;
import com.squareup.wire.schema.internal.parser.*;
import io.apicurio.registry.utils.protobuf.schema.DynamicSchema;
import io.apicurio.registry.utils.protobuf.schema.EnumDefinition;
import io.apicurio.registry.utils.protobuf.schema.MessageDefinition;
import io.apicurio.registry.utils.protobuf.schema.ProtobufSchema;
import io.axual.ksml.data.exception.SchemaException;
import lombok.Getter;

import java.util.*;

import static io.apicurio.registry.utils.protobuf.schema.FileDescriptorUtils.DEFAULT_LOCATION;

public class ProtobufWriteContext {
    private static final Syntax DEFAULT_SYNTAX = Syntax.PROTO_3;
    private static final Descriptors.FileDescriptor[] NO_DEPENDENCIES = new Descriptors.FileDescriptor[0];
    @Getter
    private final String namespace;
    private final String name;
    private final List<TypeElement> types = new ArrayList<>();
    private final Set<String> convertedTypes = new HashSet<>();

    public ProtobufWriteContext(String namespace, String name) {
        this.namespace = namespace;
        this.name = name;
    }

    public void addType(TypeElement type) {
        if (notDuplicateType(namespace, type)) {
            types.add(type);
        }
    }

    public ProtobufSchema toProtobufSchema() {
        final var options = new ArrayList<OptionElement>();
        final var fileElement = new ProtoFileElement(
                DEFAULT_LOCATION,
                namespace,
                DEFAULT_SYNTAX,
                Collections.emptyList(),
                Collections.emptyList(),
                types.reversed(),
                Collections.emptyList(),
                Collections.emptyList(),
                options);
        final var fileDescriptor = toFileDescriptor(fileElement);
        return new ProtobufSchema(fileDescriptor, fileElement);
    }

    private boolean notDuplicateType(String namespace, TypeElement type) {
        final var fullName = (namespace != null && !namespace.isEmpty() ? namespace + "." : "") + type.getName();
        final var result = convertedTypes.contains(fullName);
        convertedTypes.add(fullName);
        return !result;
    }

    private Descriptors.FileDescriptor toFileDescriptor(ProtoFileElement fileElement) {
        final var result = DynamicSchema.newBuilder();
        result.setSyntax(DEFAULT_SYNTAX.toString());
        result.setPackage(namespace);
        result.setName(name);

        // Convert all top-level messages and enums
        for (final var type : fileElement.getTypes()) {
            if (type instanceof MessageElement messageElement) {
                result.addMessageDefinition(toMessageDefinition(namespace, messageElement));
            }
            if (type instanceof EnumElement enumElement) {
                result.addEnumDefinition(toEnumDefinition(namespace, enumElement));
            }
        }

        try {
            final var ds = result.build();
            return Descriptors.FileDescriptor.buildFrom(ds.getFileDescriptorProto(), NO_DEPENDENCIES);
        } catch (Descriptors.DescriptorValidationException e) {
            throw new SchemaException("Can not generate dynamic PROTOBUF schema" + name + ": " + e.getMessage(), e);
        }
    }

    private MessageDefinition toMessageDefinition(String namespace, MessageElement messageElement) {
        // Mark the namespace + message name as done
        final var fullName = (namespace != null && !namespace.isEmpty() ? namespace + "." : "") + messageElement.getName();
        convertedTypes.add(fullName);

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
        convertedTypes.add(fullName);

        final var enumBuilder = EnumDefinition.newBuilder(enumElement.getName());
        for (final var constant : enumElement.getConstants()) {
            enumBuilder.addValue(constant.getName(), constant.getTag());
        }
        return enumBuilder.build();
    }
}
