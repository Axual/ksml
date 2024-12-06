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
import io.axual.ksml.data.schema.*;
import io.axual.ksml.data.type.Symbol;
import io.axual.ksml.data.util.ListUtil;

import java.util.*;

import static io.axual.ksml.data.notation.protobuf.ProtobufConstants.DEFAULT_LOCATION;
import static io.axual.ksml.data.notation.protobuf.ProtobufConstants.NO_DOCUMENTATION;

public class ProtobufFileElementSchemaMapper implements DataSchemaMapper<ProtoFileElement> {
    private static final int PROTOBUF_ENUM_DEFAULT_VALUE_INDEX = 0;
    private final Set<String> processedDescriptors = new HashSet<>();

    @Override
    public StructSchema toDataSchema(String namespace, String name, ProtoFileElement fileElement) {
        // Look up the message name in the schema
        final var message = findMessage(fileElement, name);
        // Create a read context and parse the message into a struct schema
        final var context = new ProtobufReadContext(fileElement);
        // Convert the message fields
        final var fields = convertMessageFieldsToDataSchema(context, message);
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

    private List<DataField> convertMessageFieldsToDataSchema(ProtobufReadContext context, MessageElement message) {
        // Get the list of oneOfs
        final var oneOfs = message.getOneOfs();
        // Map all oneOfs to their respective list of fields
        final Map<OneOfElement, List<FieldElement>> oneOfMap = new HashMap<>();
        oneOfs.forEach(oo -> oneOfMap.put(oo, oo.getFields()));
        // Collect all the fields in a flat collection
        final Set<FieldElement> oneOfFields = new HashSet<>();
        oneOfMap.forEach((key, value) -> oneOfFields.addAll(value));
        // Get the list of fields for this message
        final var messageFields = new ArrayList<>(message.getFields());
        // Remove the oneOf fields
        messageFields.removeAll(oneOfFields);

        // Convert the list of fields and oneOfs
        List<DataField> result = new ArrayList<>(messageFields.size());

        // Convert all fields and add to the result list
        for (final var field : messageFields) {
            result.add(convertFieldToDataSchema(context, field));
        }

        // Convert all oneOfs
        for (final var oneOf : oneOfMap.entrySet()) {
            // Convert the oneOf to a UnionSchema
            final var ooFields = new ArrayList<DataField>();
            for (final var field : oneOf.getValue())
                ooFields.add(convertFieldToDataSchema(context, field));
            final var oneOfUnion = new UnionSchema(ooFields.toArray(DataField[]::new));
            result.add(new DataField(oneOf.getKey().getName(), oneOfUnion));
        }

        // Return the list of converted fields
        return result;
    }

    private DataField convertFieldToDataSchema(ProtobufReadContext context, FieldElement field) {
        // Don't get a default value for an embedded message field
        final var defaultValue = field.getDefaultValue() != null
                ? field.getDefaultValue()
                : null;
        final var name = field.getName();
        final var required = field.getLabel() == null || field.getLabel() == Field.Label.REQUIRED;
        final var list = field.getLabel() == Field.Label.REPEATED;
        final var type = convertType(context, field);
        if (type == null) {
            throw new SchemaException("Schema for field '" + field.getName() + "' can not be NULL");
        }
        return new DataField(name, list ? new ListSchema(type) : type, field.getDocumentation(), field.getTag(), required);
    }

    private DataSchema convertType(ProtobufReadContext context, FieldElement field) {
        switch (field.getType()) {
            case "double":
                return DataSchema.doubleSchema();
            case "float":
                return DataSchema.floatSchema();
            case "int64":
                return DataSchema.longSchema();
            case "uint64":
                return DataSchema.longSchema();
            case "int32":
                return DataSchema.integerSchema();
            case "fixed64":
                return DataSchema.longSchema();
            case "fixed32":
                return DataSchema.integerSchema();
            case "boolean":
                return DataSchema.booleanSchema();
            case "string":
                return DataSchema.stringSchema();
            case "bytes":
                return DataSchema.bytesSchema();
            case "uint32":
                return DataSchema.integerSchema();
            case "sfixed32":
                return DataSchema.integerSchema();
            case "sfixed64":
                return DataSchema.longSchema();
            case "sint32":
                return DataSchema.integerSchema();
            case "sint64":
                return DataSchema.longSchema();
        }

        // Look up the non-standard type
        if (!field.getType().isEmpty()) {
            final var findResult = context.type(field.getType());
            if (findResult != null && findResult.type() instanceof EnumElement enumElement) {
                final var symbols = enumElement.getConstants().stream().map(constant -> new Symbol(constant.getName(), constant.getDocumentation(), constant.getTag())).toList();
                if (symbols.isEmpty()) {
                    throw new SchemaException("Protobuf enum type '" + enumElement.getName() + "' has no constants defined");
                }
                final var defaultValue = ListUtil.find(symbols, symbol -> symbol.index() == PROTOBUF_ENUM_DEFAULT_VALUE_INDEX);
                return new EnumSchema(findResult.namespace(), enumElement.getName(), findResult.type().getDocumentation(), symbols, defaultValue != null ? defaultValue.name() : null);
            }
            if (findResult != null && findResult.type() instanceof MessageElement msgElement) {
                final var fields = convertMessageFieldsToDataSchema(context, msgElement);
                return new StructSchema(findResult.namespace(), msgElement.getName(), "", fields);
            }
        }

        throw new SchemaException("Protobuf field '" + field.getName() + "' has unknown type '" + field.getType() + "'");
    }

    @Override
    public ProtoFileElement fromDataSchema(DataSchema schema) {
        final var name = schema instanceof NamedSchema namedSchema ? " '" + namedSchema.name() + "'" : null;

        if (schema instanceof StructSchema structSchema) {
            final var context = new ProtobufWriteContext(structSchema.namespace());
            final var message = convertToMessageElement(context, structSchema);
            context.addType(message);
            return context.toProtoFileElement();
        }

        throw new SchemaException("Can not convert " + schema.type() + " into dynamic PROTOBUF schema" + name);
    }

    private MessageElement convertToMessageElement(ProtobufWriteContext context, StructSchema schema) {
        final var nestedTypes = new ArrayList<TypeElement>();
        final var oneOfs = new ArrayList<OneOfElement>();

        final var fields = new ArrayList<FieldElement>();
        for (final var field : schema.fields()) {
            final var type = convertFieldToType(context, nestedTypes, oneOfs, schema.name(), field);
            if (type != null) fields.add(convertToFieldElement(field, type));
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

    private static FieldElement convertToFieldElement(DataField field, String type) {
        final var required = field.required();
        final var list = field.schema().type() == DataSchema.Type.LIST;
        final var defaultValue = field.defaultValue() != null ? field.defaultValue().toString() : null;
        return new FieldElement(
                DEFAULT_LOCATION,
                required ? null : list ? Field.Label.REPEATED : Field.Label.OPTIONAL,
                type,
                field.name(),
                defaultValue,
                null,
                field.index(),
                field.doc(),
                Collections.emptyList());
    }

    private String convertFieldToType(ProtobufWriteContext context, List<TypeElement> parentNestedTypes, List<OneOfElement> parentOneOfs, String parentName, DataField field) {
        if (field.schema() instanceof UnionSchema unionSchema) {
            final var valueTypes = new ArrayList<FieldElement>();
            for (int index = 0; index < unionSchema.valueTypes().length; index++) {
                final var vt = unionSchema.valueTypes()[index];
                final var valueType = new FieldElement(
                        DEFAULT_LOCATION,
                        Field.Label.ONE_OF,
                        convertFieldToType(context, parentNestedTypes, parentOneOfs, parentName, vt),
                        vt.name(),
                        vt.defaultValue() != null ? vt.defaultValue().toString() : null,
                        null,
                        vt.index(),
                        vt.doc(),
                        Collections.emptyList());
                valueTypes.add(valueType);
            }

            final var oneOf = new OneOfElement(field.name(), field.doc() != null ? field.doc() : "", valueTypes, Collections.emptyList(), Collections.emptyList(), DEFAULT_LOCATION);
            parentOneOfs.add(oneOf);
        }

        return convertSchemaToType(context, parentNestedTypes, parentName, field.schema());
    }

    private String convertSchemaToType(ProtobufWriteContext context, List<TypeElement> parentNestedTypes, String
            parentName, DataSchema schema) {
        if (schema instanceof EnumSchema enumSchema) {
            final var enm = convertToEnumElement(enumSchema);
            // Find out if the enum is nested, or defined at top level
            if (enumSchema.namespace() != null && enumSchema.namespace().equals(context.namespace + "." + parentName)) {
                if (notDuplicate(enumSchema.fullName())) {
                    parentNestedTypes.add(enm);
                }
            } else {
                context.addType(enm);
            }
            return enumSchema.name();
        }
        if (schema instanceof ListSchema listSchema) {
            // The repeated label is caught above, so only convert the value schema to a type
            return convertSchemaToType(context, parentNestedTypes, parentName, listSchema.valueSchema());
        }
        if (schema instanceof StructSchema structSchema) {
            final var message = convertToMessageElement(context, structSchema);
            // Find out if the message is nested, or defined at top level
            if (structSchema.namespace() != null && structSchema.namespace().equals(context.namespace + "." + parentName)) {
                if (notDuplicate(structSchema.fullName()))
                    parentNestedTypes.add(message);
            } else {
                context.addType(message);
            }
            return structSchema.name();
        }
        return switch (schema.type()) {
            case BOOLEAN -> "boolean";
            case BYTE, SHORT, INTEGER -> "int32";
            case LONG -> "int64";
            case DOUBLE -> "double";
            case FLOAT -> "float";
            case STRING -> "string";
            case MAP -> null;
            case UNION -> null;
            default -> throw new SchemaException("Can not convert schema type " + schema.type() + " to PROTOBUF type");
        };
    }

    private EnumElement convertToEnumElement(EnumSchema schema) {
        final var constants = schema.symbols().stream().map(symbol -> new EnumConstantElement(DEFAULT_LOCATION, symbol.name(), symbol.index(), symbol.hasDoc() ? symbol.doc() : NO_DOCUMENTATION, Collections.emptyList())).toList();
        return new EnumElement(DEFAULT_LOCATION, schema.name(), schema.hasDoc() ? schema.doc() : NO_DOCUMENTATION, Collections.emptyList(), constants, Collections.emptyList());
    }

    private boolean notDuplicate(String name) {
        final var result = !processedDescriptors.contains(name);
        processedDescriptors.add(name);
        return result;
    }

    ////////////////////////////////////////////////////////////////////////////
    // READ CONTEXT
    ////////////////////////////////////////////////////////////////////////////

    public static class ProtobufReadContext {
        private final ProtoFileElement fileElement;
        private final String namespace;

        public ProtobufReadContext(ProtoFileElement fileElement) {
            this.fileElement = fileElement;
            this.namespace = fileElement.getPackageName();
        }

        public record FindResult(String namespace, TypeElement type) {
        }

        public FindResult type(String name) {
            final var descriptor = findType(fileElement.getPackageName(), fileElement.getTypes(), name);
            if (descriptor != null) return descriptor;
            final var enm = ListUtil.find(fileElement.getTypes(), type -> type.getName().equals(namespace + "." + name));
            if (enm != null) return new FindResult(namespace, enm);
            return null;
        }

        private FindResult findType(String namespace, List<TypeElement> types, String name) {
            for (final var type : types) {
                if (type.getName().equals(name)) return new FindResult(namespace, type);
                final var subMsg = findType(namespace + "." + type.getName(), type.getNestedTypes(), name);
                if (subMsg != null) return subMsg;
            }
            return null;
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    // WRITE CONTEXT
    ////////////////////////////////////////////////////////////////////////////

    private static class ProtobufWriteContext {
        private final String namespace;
        private final List<TypeElement> types = new ArrayList<>();

        public ProtobufWriteContext(String namespace) {
            this.namespace = namespace;
        }

        public void addType(TypeElement type) {
            final var fullName = (namespace != null && !namespace.isEmpty() ? namespace + "." : "") + type.getName();
            if (ListUtil.find(types, t -> t.getName().equals(fullName)) == null) {
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
