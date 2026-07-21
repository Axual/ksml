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

import com.google.protobuf.Descriptors;
import com.squareup.wire.schema.Field;
import com.squareup.wire.schema.internal.parser.EnumConstantElement;
import com.squareup.wire.schema.internal.parser.EnumElement;
import com.squareup.wire.schema.internal.parser.FieldElement;
import com.squareup.wire.schema.internal.parser.MessageElement;
import com.squareup.wire.schema.internal.parser.OneOfElement;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import com.squareup.wire.schema.internal.parser.TypeElement;

import java.util.ArrayList;
import java.util.Collections;

import static io.axual.ksml.data.notation.protobuf.ProtobufConstants.DEFAULT_LOCATION;
import static io.axual.ksml.data.notation.protobuf.ProtobufConstants.DEFAULT_SYNTAX;
import static io.axual.ksml.data.notation.protobuf.ProtobufConstants.NO_DOCUMENTATION;

/**
 * Converts a compiled protobuf {@link Descriptors.Descriptor} back into a Wire {@link ProtoFileElement}.
 *
 * <p>This direction depends only on the protobuf runtime and the Wire element model, not on any
 * schema-registry vendor, so it is shared by the Apicurio and Confluent protobuf mappers.</p>
 */
public class DescriptorToFileElementConverter {
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
                final Field.Label label;
                if (oneOfField.isRequired()) {
                    label = Field.Label.REQUIRED;
                } else {
                    label = oneOfField.isRepeated() ? Field.Label.REPEATED : Field.Label.OPTIONAL;
                }
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

    @SuppressWarnings("java:S3358")
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
