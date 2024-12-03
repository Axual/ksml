package io.axual.ksml.data.notation.protobuf;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2023 Axual B.V.
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
import io.apicurio.registry.utils.protobuf.schema.EnumDefinition;
import io.apicurio.registry.utils.protobuf.schema.MessageDefinition;
import io.apicurio.registry.utils.protobuf.schema.ProtobufSchema;
import io.axual.ksml.data.exception.SchemaException;
import io.axual.ksml.data.mapper.DataSchemaMapper;
import io.axual.ksml.data.schema.*;
import io.axual.ksml.data.util.MapUtil;

import java.util.*;
import java.util.stream.Collectors;

public class ProtobufSchemaMapper implements DataSchemaMapper<ProtobufSchema> {
    private static final int PROTOBUF_ENUM_DEFAULT_VALUE_INDEX = 0;
    private final Set<String> processedDescriptors = new HashSet<>();

    @Override
    public StructSchema toDataSchema(String namespace, String name, ProtobufSchema schema) {
        // Look up the message name in the schema
        Descriptors.Descriptor message = findMessage(schema, name);

        // Create a read context and parse the message into a struct schema
        final var context = new ProtobufReadContext(schema);
        // Convert the message fields
        final var fields = convertMessageFieldsToDataSchema(context, message);
        // Return a new struct schema with the converted fields
        return new StructSchema(context.namespace(), message.getName(), "", fields);
    }

    private static Descriptors.Descriptor findMessage(ProtobufSchema schema, String name) {
        // Find the message by name in the schema's message types
        for (final var msg : schema.getFileDescriptor().getMessageTypes()) {
            if (msg.getName().equals(name)) return msg;
        }
        throw new SchemaException("Could not find message of type '" + name + "' in PROTOBUF schema '" + schema.getFileDescriptor().getName() + "'");
    }

    private List<DataField> convertMessageFieldsToDataSchema(ProtobufReadContext context, Descriptors.Descriptor message) {
        // Get the list of oneOfs
        final var oneOfs = message.getRealOneofs();
        // Map all oneOfs to their respective list of fields
        final Map<Descriptors.OneofDescriptor, List<Descriptors.FieldDescriptor>> oneOfMap = new HashMap<>();
        oneOfs.forEach(oo -> oneOfMap.put(oo, oo.getFields()));
        // Collect all the fields in a flat collection
        final Set<Descriptors.FieldDescriptor> oneOfFields = new HashSet<>();
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

    private DataField convertFieldToDataSchema(ProtobufReadContext context, Descriptors.FieldDescriptor field) {
        // Don't get a default value for an embedded message field
        final var defaultValue = field.getContainingType() == null
                ? field.getDefaultValue()
                : null;
        final var name = field.getName();
        final var required = !field.toProto().hasLabel() || field.toProto().getLabel() == DescriptorProtos.FieldDescriptorProto.Label.LABEL_REQUIRED;
        final var list = field.toProto().hasLabel() && field.toProto().getLabel() == DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED;
        final var type = convertType(context, field);
        if (type == null) {
            throw new SchemaException("Schema for field '" + field.getName() + "' can not be NULL");
        }
        if (defaultValue != null) {

        }
        return new DataField(name, list ? new ListSchema(type) : type, "", field.getNumber(), required);
    }

    private DataSchema convertType(ProtobufReadContext context, Descriptors.FieldDescriptor field) {
        final var typeName = field.toProto().getTypeName();
        if (!typeName.isEmpty()) {
            final var findResult = context.type(typeName);
            if (findResult != null && findResult.descriptor() instanceof Descriptors.EnumDescriptor enumElement) {
                final var symbols = enumElement.getValues().stream()
                        .collect(
                                Collectors.toMap(
                                        Descriptors.EnumValueDescriptor::getName,
                                        Descriptors.EnumValueDescriptor::getNumber));
                if (symbols.isEmpty()) {
                    throw new SchemaException("Protobuf enum type '" + enumElement.getName() + "' has no constants defined");
                }
                final var reverseSymbols = MapUtil.invertMap(symbols);
                return new EnumSchema(findResult.namespace(), enumElement.getName(), "", symbols, reverseSymbols.get(PROTOBUF_ENUM_DEFAULT_VALUE_INDEX));
            }
            if (findResult != null && findResult.descriptor() instanceof Descriptors.Descriptor msgElement) {
                final var fields = convertMessageFieldsToDataSchema(context, msgElement);
                return new StructSchema(findResult.namespace(), msgElement.getName(), "", fields);
            }
            return null;
        }
        return switch (field.toProto().getType()) {
            case TYPE_DOUBLE -> DataSchema.doubleSchema();
            case TYPE_FLOAT -> DataSchema.floatSchema();
            case TYPE_INT64 -> DataSchema.longSchema();
            case TYPE_UINT64 -> DataSchema.longSchema();
            case TYPE_INT32 -> DataSchema.integerSchema();
            case TYPE_FIXED64 -> DataSchema.longSchema();
            case TYPE_FIXED32 -> DataSchema.integerSchema();
            case TYPE_BOOL -> DataSchema.booleanSchema();
            case TYPE_STRING -> DataSchema.stringSchema();
            case TYPE_BYTES -> DataSchema.bytesSchema();
            case TYPE_UINT32 -> DataSchema.integerSchema();
            case TYPE_ENUM -> new EnumSchema(context.namespace(), field.getName(), "", new ArrayList<>());
            case TYPE_SFIXED32 -> DataSchema.integerSchema();
            case TYPE_SFIXED64 -> DataSchema.longSchema();
            case TYPE_SINT32 -> DataSchema.integerSchema();
            case TYPE_SINT64 -> DataSchema.longSchema();
            // Skipped TYPE_GROUP and TYPE_MESSAGE
            default -> throw new SchemaException("Unknown type '" + field.getName() + "'");
        };
    }

    @Override
    public ProtobufSchema fromDataSchema(DataSchema schema) {
        final var name = schema instanceof NamedSchema namedSchema ? " '" + namedSchema.name() + "'" : null;

        if (schema instanceof StructSchema structSchema) {
            final var context = new ProtobufWriteContext(structSchema.namespace(), structSchema.name());
            final var message = convertToMessage(context, structSchema);
            context.addMessage(structSchema.name(), message);
            return context.toProtobufSchema();
        }

        throw new SchemaException("Can not convert " + schema.type() + " into dynamic PROTOBUF schema" + name);
    }

    private MessageDefinition convertToMessage(ProtobufWriteContext context, StructSchema schema) {
        final var result = MessageDefinition.newBuilder(schema.name());
        for (final var field : schema.fields()) {
            final var type = convertFieldToType(context, result, schema.name(), field);
            if (type != null) {
                final var required = field.required();
                final var list = field.schema().type() == DataSchema.Type.LIST;
                final var label = required ? "required" : list ? "repeated" : "optional";
                final var defaultValue = field.defaultValue() != null ? field.defaultValue().toString() : null;
                result.addField(label, type, field.name(), field.index(), defaultValue);
            }
        }
        return result.build();
    }

    private String convertFieldToType(ProtobufWriteContext context, MessageDefinition.Builder parent, String
            parentName, DataField field) {
        if (field.schema() instanceof UnionSchema unionSchema) {
            final var oneofBuilder = parent.addOneof(field.name());
            for (int index = 0; index < unionSchema.valueTypes().length; index++) {
                final var valueType = unionSchema.valueTypes()[index];
                oneofBuilder.addField(convertFieldToType(context, parent, parentName, valueType), valueType.name(), valueType.index(), null);
            }
        }
        return convertSchemaToType(context, parent, parentName, field.schema());
    }

    private String convertSchemaToType(ProtobufWriteContext context, MessageDefinition.Builder parent, String
            parentName, DataSchema schema) {
        if (schema instanceof EnumSchema enumSchema) {
            final var enm = EnumDefinition.newBuilder(enumSchema.name());
            enumSchema.symbols().forEach(enm::addValue);
            // Find out if the enum is nested, or defined at top level
            if (enumSchema.namespace() != null && enumSchema.namespace().equals(context.namespace() + "." + parentName)) {
                if (notDuplicate(enumSchema.fullName())) parent.addEnumDefinition(enm.build());
            } else {
                context.addEnum(enumSchema.name(), enm.build());
            }
            return enumSchema.name();
        }
        if (schema instanceof ListSchema listSchema) {
            // The repeated label is caught above, so only convert the value schema to a type
            return convertSchemaToType(context, parent, parentName, listSchema.valueSchema());
        }
        if (schema instanceof StructSchema structSchema) {
            final var message = convertToMessage(context, structSchema);
            if (structSchema.namespace() != null && structSchema.namespace().equals(context.namespace() + "." + parentName)) {
                if (notDuplicate(structSchema.fullName()))
                    parent.addMessageDefinition(message);
            } else {
                context.addMessage(structSchema.name(), message);
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

    private boolean notDuplicate(String name) {
        final var result = !processedDescriptors.contains(name);
        processedDescriptors.add(name);
        return result;
    }
}
