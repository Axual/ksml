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

import com.squareup.wire.schema.Field;
import com.squareup.wire.schema.internal.parser.*;
import io.apicurio.registry.utils.protobuf.schema.FileDescriptorUtils;
import io.axual.ksml.data.exception.SchemaException;
import io.axual.ksml.data.mapper.DataSchemaMapper;
import io.axual.ksml.data.notation.ReferenceResolver;
import io.axual.ksml.data.schema.*;
import io.axual.ksml.data.type.Symbol;
import io.axual.ksml.data.util.ListUtil;

import java.util.*;

import static io.axual.ksml.data.notation.protobuf.ProtobufConstants.DEFAULT_LOCATION;
import static io.axual.ksml.data.notation.protobuf.ProtobufConstants.NO_DOCUMENTATION;

public class ProtobufFileElementSchemaMapper implements DataSchemaMapper<ProtoFileElement> {
    private static final int PROTOBUF_ENUM_DEFAULT_VALUE_INDEX = 0;

    @Override
    public StructSchema toDataSchema(String namespace, String name, ProtoFileElement fileElement) {
        // Look up the message name in the schema
        final var message = findMessage(fileElement, name);
        // Create a read context and parse the message into a struct schema
        final var context = new ProtobufReadContext(fileElement);
        // Convert the message fields
        final var fields = convertMessageFieldsToDataFields(context, message);
        // Return a new struct schema with the converted fields
        return new StructSchema(context.namespace, message.getName(), message.getDocumentation(), fields);
    }

    private static MessageElement findMessage(ProtoFileElement fileElement, String name) {
        // Find the message by name in the schema's message types
        for (final var msg : fileElement.getTypes()) {
            if (msg instanceof MessageElement msgElement && msgElement.getName().equals(name)) return msgElement;
        }
        throw new SchemaException("Could not find message of type '" + name + "' in PROTOBUF schema");
    }

    private List<DataField> convertMessageFieldsToDataFields(ProtobufReadContext context, MessageElement message) {
        // Get the list of oneOfs
        final var oneOfs = message.getOneOfs();
        // Map all oneOfs to their respective list of fields
        final Map<OneOfElement, List<FieldElement>> oneOfMap = new HashMap<>();
        oneOfs.forEach(oo -> oneOfMap.put(oo, oo.getFields()));
        // Get the list of fields for this message
        final var messageFields = new ArrayList<>(message.getFields());
        // Remove the oneOf fields from the message fields
        oneOfMap.forEach((key, value) -> messageFields.removeAll(value));

        // Convert the list of message fields and oneOfs
        final var result = new ArrayList<DataField>(messageFields.size());

        // Add all converted message fields to the result list
        for (final var field : messageFields) {
            result.add(convertFieldElementToDataField(context, field));
        }

        // Add all converted oneOfs to the result list
        for (final var oneOf : oneOfMap.entrySet()) {
            // Convert the oneOf to a UnionSchema
            final var ooFields = new ArrayList<DataField>();
            for (final var field : oneOf.getValue())
                ooFields.add(convertFieldElementToDataField(context, field));
            final var oneOfUnion = new UnionSchema(ooFields.toArray(DataField[]::new));
            result.add(new DataField(oneOf.getKey().getName(), oneOfUnion, oneOf.getKey().getDocumentation()));
        }

        // Return the list of converted fields
        return result;
    }

    private DataField convertFieldElementToDataField(ProtobufReadContext context, FieldElement field) {
        // Don't get a default value for an embedded message field
        final var defaultValue = field.getDefaultValue() != null ? field.getDefaultValue() : null;
        final var name = field.getName();
        final var required = field.getLabel() == null || field.getLabel() == Field.Label.REQUIRED;
        final var list = field.getLabel() == Field.Label.REPEATED;
        final var type = convertFieldElementToDataSchema(context, field);
        if (type == null) {
            throw new SchemaException("Schema for field '" + field.getName() + "' can not be NULL");
        }
        return new DataField(name, list ? new ListSchema(type) : type, field.getDocumentation(), field.getTag(), required, false, defaultValue != null ? new DataValue(defaultValue) : null);
    }

    private DataSchema convertFieldElementToDataSchema(ProtobufReadContext context, FieldElement field) {
        switch (field.getType()) {
            case "boolean":
                return DataSchema.BOOLEAN_SCHEMA;
            case "int32", "fixed32", "sfixed32", "sint32", "uint32":
                return DataSchema.INTEGER_SCHEMA;
            case "int64", "fixed64", "sfixed64", "sint64", "uint64":
                return DataSchema.LONG_SCHEMA;
            case "float":
                return DataSchema.FLOAT_SCHEMA;
            case "double":
                return DataSchema.DOUBLE_SCHEMA;
            case "string":
                return DataSchema.STRING_SCHEMA;
            case "bytes":
                return DataSchema.BYTES_SCHEMA;
            default:
        }

        // Look up the non-standard type
        if (!field.getType().isEmpty()) {
            final var reference = context.get(field.getType());
            if (reference != null && reference.type() instanceof EnumElement enumElement) {
                final var symbols = enumElement.getConstants().stream().map(constant -> new Symbol(constant.getName(), constant.getDocumentation(), constant.getTag())).toList();
                if (symbols.isEmpty()) {
                    throw new SchemaException("Protobuf enum type '" + enumElement.getName() + "' has no constants defined");
                }
                final var defaultValue = ListUtil.find(symbols, symbol -> symbol.tag() == PROTOBUF_ENUM_DEFAULT_VALUE_INDEX);
                return new EnumSchema(reference.namespace(), enumElement.getName(), reference.type().getDocumentation(), symbols, defaultValue != null ? defaultValue.name() : null);
            }
            if (reference != null && reference.type() instanceof MessageElement msgElement) {
                final var fields = convertMessageFieldsToDataFields(context, msgElement);
                return new StructSchema(reference.namespace(), msgElement.getName(), "", fields);
            }
        }

        throw new SchemaException("Protobuf field '" + field.getName() + "' has unknown type '" + field.getType() + "'");
    }

    @Override
    public ProtoFileElement fromDataSchema(DataSchema schema) {
        final var name = schema instanceof NamedSchema namedSchema ? " '" + namedSchema.name() + "'" : null;

        if (schema instanceof StructSchema structSchema) {
            final var context = new ProtobufWriteContext(structSchema.namespace());
            final var message = convertStructSchemaToMessageElement(context, structSchema);
            context.addType("", message);
            return context.toProtoFileElement();
        }

        throw new SchemaException("Can not convert " + schema.type() + " into dynamic PROTOBUF schema" + name);
    }

    private MessageElement convertStructSchemaToMessageElement(ProtobufWriteContext context, StructSchema schema) {
        final var nestedTypes = new ArrayList<TypeElement>();
        final var oneOfs = new ArrayList<OneOfElement>();

        final var fields = new ArrayList<FieldElement>();
        for (final var field : schema.fields()) {
            final var type = convertDataFieldToProtoType(context, nestedTypes, oneOfs, schema.name(), field);
            if (type != null) fields.add(convertDataFieldToFieldElement(field, type));
        }

        return new MessageElement(
                DEFAULT_LOCATION,
                schema.name(),
                schema.hasDoc() ? schema.doc() : NO_DOCUMENTATION,
                nestedTypes,
                Collections.emptyList(),
                Collections.emptyList(),
                fields,
                oneOfs,
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList());
    }

    private static FieldElement convertDataFieldToFieldElement(DataField field, String type) {
        final var required = field.required();
        final var list = field.schema() instanceof ListSchema;
        final var defaultValue = field.defaultValue() != null ? field.defaultValue().toString() : null;
        return new FieldElement(
                DEFAULT_LOCATION,
                required ? null : list ? Field.Label.REPEATED : Field.Label.OPTIONAL,
                type,
                field.name(),
                defaultValue,
                null,
                field.tag(),
                field.doc(),
                Collections.emptyList());
    }

    private String convertDataFieldToProtoType(ProtobufWriteContext context, List<TypeElement> parentNestedTypes, List<OneOfElement> parentOneOfs, String parentName, DataField field) {
        if (field.schema() instanceof UnionSchema unionSchema) {
            final var memberTypes = new ArrayList<FieldElement>();
            for (int index = 0; index < unionSchema.memberSchemas().length; index++) {
                final var memberSchema = unionSchema.memberSchemas()[index];
                final var memberType = new FieldElement(
                        DEFAULT_LOCATION,
                        null,
                        convertDataFieldToProtoType(context, parentNestedTypes, parentOneOfs, parentName, memberSchema),
                        memberSchema.name(),
                        memberSchema.defaultValue() != null ? memberSchema.defaultValue().toString() : null,
                        null,
                        memberSchema.tag(),
                        memberSchema.doc(),
                        Collections.emptyList());
                memberTypes.add(memberType);
            }

            final var oneOf = new OneOfElement(field.name(), field.doc() != null ? field.doc() : "", memberTypes, Collections.emptyList(), Collections.emptyList(), DEFAULT_LOCATION);
            parentOneOfs.add(oneOf);
        }

        return convertDataSchemaToProtoType(context, parentNestedTypes, parentName, field.schema());
    }

    private String convertDataSchemaToProtoType(ProtobufWriteContext context, List<TypeElement> parentNestedTypes, String parentName, DataSchema schema) {
        if (schema == DataSchema.BOOLEAN_SCHEMA) return "boolean";
        if (schema == DataSchema.BYTE_SCHEMA || schema == DataSchema.SHORT_SCHEMA || schema == DataSchema.INTEGER_SCHEMA)
            return "int32";
        if (schema == DataSchema.LONG_SCHEMA) return "int64";
        if (schema == DataSchema.FLOAT_SCHEMA) return "float";
        if (schema == DataSchema.DOUBLE_SCHEMA) return "double";
        if (schema == DataSchema.BYTES_SCHEMA || schema instanceof FixedSchema) return "bytes";
        if (schema == DataSchema.STRING_SCHEMA) return "string";
        if (schema instanceof EnumSchema enumSchema) {
            final var enm = convertEnumSchemaToEnumElement(enumSchema);
            // Find out if the enum is nested, or defined at top level
            if (enumSchema.namespace() != null && enumSchema.namespace().equals(context.namespace + "." + parentName)) {
                if (context.notDuplicate(enumSchema.fullName())) {
                    parentNestedTypes.add(enm);
                }
            } else {
                context.addType(parentName, enm);
            }
            return enumSchema.name();
        }
        if (schema instanceof ListSchema listSchema) {
            // The repeated label is caught above, so only convert the value schema to a type
            return convertDataSchemaToProtoType(context, parentNestedTypes, parentName, listSchema.valueSchema());
        }
        if (schema instanceof MapSchema) return null;
        if (schema instanceof StructSchema structSchema) {
            final var message = convertStructSchemaToMessageElement(context, structSchema);
            // Find out if the message is nested, or defined at top level
            if (structSchema.namespace() != null && structSchema.namespace().equals(context.namespace + "." + parentName)) {
                if (context.notDuplicate(structSchema.fullName()))
                    parentNestedTypes.add(message);
            } else {
                context.addType(parentName, message);
            }
            return structSchema.name();
        }
        if (schema instanceof UnionSchema) return null;
        throw new SchemaException("Can not convert schema type " + schema.type() + " to PROTOBUF type");
    }

    private EnumElement convertEnumSchemaToEnumElement(EnumSchema schema) {
        final var constants = schema.symbols().stream().map(symbol -> new EnumConstantElement(DEFAULT_LOCATION, symbol.name(), symbol.tag(), symbol.hasDoc() ? symbol.doc() : NO_DOCUMENTATION, Collections.emptyList())).toList();
        return new EnumElement(DEFAULT_LOCATION, schema.name(), schema.hasDoc() ? schema.doc() : NO_DOCUMENTATION, Collections.emptyList(), constants, Collections.emptyList());
    }

    /**
     * This is a helper class to convert from ProtoFileElements to DataSchema with type lookups
     */
    private record ReadContextReference(String namespace, TypeElement type) {
    }

    private static class ProtobufReadContext implements ReferenceResolver<ReadContextReference> {
        private final ProtoFileElement fileElement;
        private final String namespace;

        public ProtobufReadContext(ProtoFileElement fileElement) {
            this.fileElement = fileElement;
            this.namespace = fileElement.getPackageName();
        }

        public ReadContextReference get(String name) {
            final var descriptor = getFrom(fileElement.getPackageName(), fileElement.getTypes(), name);
            if (descriptor != null) return descriptor;
            final var enm = ListUtil.find(fileElement.getTypes(), type -> type.getName().equals(namespace + "." + name));
            if (enm != null) return new ReadContextReference(namespace, enm);
            return null;
        }

        private ReadContextReference getFrom(String namespace, List<TypeElement> types, String name) {
            for (final var type : types) {
                if (type.getName().equals(name)) return new ReadContextReference(namespace, type);
                final var subMsg = getFrom(namespace + "." + type.getName(), type.getNestedTypes(), name);
                if (subMsg != null) return subMsg;
            }
            return null;
        }
    }

    /**
     * This is a helper class to convert from DataSchema to ProtoFileElements without type duplication
     */
    private static class ProtobufWriteContext {
        private final String namespace;
        private final Set<String> typeNames = new HashSet<>();
        private final List<TypeElement> types = new ArrayList<>();
        private final Set<String> processedDescriptors = new HashSet<>();

        public ProtobufWriteContext(String namespace) {
            this.namespace = namespace;
        }

        private boolean notDuplicate(String name) {
            final var result = !processedDescriptors.contains(name);
            processedDescriptors.add(name);
            return result;
        }

        public void addType(String parentNamespace, TypeElement type) {
            final var nsPrefix = namespace != null && !namespace.isEmpty() ? namespace + "." : "";
            final var parentPrefix = parentNamespace != null && !parentNamespace.isEmpty() ? parentNamespace + "." : "";
            final var fullName = nsPrefix + parentPrefix + type.getName();
            if (!typeNames.contains(fullName)) {
                typeNames.add(fullName);
                types.add(type);
            }
        }

        public ProtoFileElement toProtoFileElement() {
            final var options = new ArrayList<OptionElement>();
            return new ProtoFileElement(
                    FileDescriptorUtils.DEFAULT_LOCATION,
                    namespace,
                    ProtobufConstants.DEFAULT_SYNTAX,
                    Collections.emptyList(),
                    Collections.emptyList(),
                    types.reversed(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    options);
        }
    }
}
