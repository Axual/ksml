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
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.schema.*;
import io.axual.ksml.data.util.MapUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

// First attempt at providing an internal schema class. The implementation relies heavily on Protobuf
// at the moment, which is fine for now, but may change in the future.
public class ProtobufSchemaMapper implements DataSchemaMapper<ProtobufSchema> {
    private static final ProtobufDataObjectMapper protobufMapper = new ProtobufDataObjectMapper();
    private static final NativeDataObjectMapper NATIVE_MAPPER = new NativeDataObjectMapper();
    private static final int PROTOBUF_ENUM_DEFAULT_VALUE_INDEX = 0;

    @Override
    public StructSchema toDataSchema(String namespace, String name, ProtobufSchema schema) {
        // The namespace and name fields are ignored, since they are already contained in the schema and
        // take precedence over the parameters to this method.
        Descriptors.Descriptor message = null;
        for (final var msg : schema.getFileDescriptor().getMessageTypes()) {
            final var msgName = msg.getName();
            if (msgName.equals(name)) {
                message = msg;
                break;
            }
        }
        if (message != null) {
            final var context = new ProtobufReadContext(schema);
            final var fields = convertFieldsToDataSchema(context, message);
            return new StructSchema(context.namespace(), message.getName(), "", fields);
        }
        return null;
    }

    private List<DataField> convertFieldsToDataSchema(ProtobufReadContext context, Descriptors.Descriptor message) {
        List<DataField> result = new ArrayList<>(10);
        for (final var field : message.toProto().getFieldList()) {
            result.add(convertFieldToDataSchema(context, field));
        }
        return result;
    }

    private DataField convertFieldToDataSchema(ProtobufReadContext context, DescriptorProtos.FieldDescriptorProto field) {
        final var defaultValue = field.getDefaultValue();
        final var name = field.getName();
        final var required = field.getLabel() == DescriptorProtos.FieldDescriptorProto.Label.LABEL_REQUIRED;
        final var list = field.getLabel() == DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED;
        final var type = convertType(context, field);
        if (type == null) {
            throw new SchemaException("Schema for field '" + field.getName() + "' can not be NULL");
        }
        if (defaultValue != null) {

        }
        return new DataField(name, list ? new ListSchema(type) : type, "", field.getNumber(), required);
    }

    private DataSchema convertType(ProtobufReadContext context, DescriptorProtos.FieldDescriptorProto field) {
        final var typeName = field.getTypeName();
        if (!typeName.isEmpty()) {
            final var type = context.type(typeName);
            if (type instanceof Descriptors.EnumDescriptor enumElement) {
                final var symbols = enumElement.getValues().stream()
                        .collect(
                                Collectors.toMap(
                                        Descriptors.EnumValueDescriptor::getName,
                                        Descriptors.EnumValueDescriptor::getNumber));
                if (symbols.isEmpty()) {
                    throw new SchemaException("Protobuf enum type '" + type.getName() + "' has no constants defined");
                }
                final var reverseSymbols = MapUtil.invertMap(symbols);
                return new EnumSchema(context.namespace(), type.getName(), "", symbols, reverseSymbols.get(PROTOBUF_ENUM_DEFAULT_VALUE_INDEX));
            }
            return null;
        }
        return switch (field.getType()) {
            case TYPE_DOUBLE -> DataSchema.doubleSchema();
            case TYPE_FLOAT -> DataSchema.floatSchema();
            case TYPE_INT64 -> DataSchema.longSchema();
            case TYPE_UINT64 -> DataSchema.longSchema();
            case TYPE_INT32 -> DataSchema.integerSchema();
            case TYPE_FIXED64 -> DataSchema.longSchema();
            case TYPE_FIXED32 -> DataSchema.integerSchema();
            case TYPE_BOOL -> DataSchema.booleanSchema();
            case TYPE_STRING -> DataSchema.stringSchema();
            case TYPE_GROUP -> null;
            case TYPE_MESSAGE -> {
                final var nestedName = field.getName();
                yield new StructSchema(context.namespace(), nestedName, "", convertFieldsToDataSchema(context, null));
            }
            case TYPE_BYTES -> DataSchema.bytesSchema();
            case TYPE_UINT32 -> DataSchema.integerSchema();
            case TYPE_ENUM -> new EnumSchema(context.namespace(), field.getName(), "", new ArrayList<>());
            case TYPE_SFIXED32 -> DataSchema.integerSchema();
            case TYPE_SFIXED64 -> DataSchema.longSchema();
            case TYPE_SINT32 -> DataSchema.integerSchema();
            case TYPE_SINT64 -> DataSchema.longSchema();
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
            final var required = field.required();
            final var list = field.schema().type() == DataSchema.Type.LIST;
            final var label = required ? "required" : list ? "repeated" : "optional";
            final var type = convertToType(context, field.schema());
            final var defaultValue = field.defaultValue() != null ? field.defaultValue().toString() : null;
            result.addField(label, type, field.name(), field.index(), defaultValue);
        }
        return result.build();
    }

    private String convertToType(ProtobufWriteContext context, DataSchema schema) {
        if (schema instanceof EnumSchema enumSchema) {
            final var enm = EnumDefinition.newBuilder(enumSchema.name());
            enumSchema.symbols().forEach(enm::addValue);
            context.addEnum(enumSchema.name(), enm.build());
            //TODO: recurse into Enum here
            return enumSchema.name();
        }
        if (schema instanceof ListSchema listSchema) {
            return convertToType(context, listSchema.valueSchema());
        }
        if (schema instanceof StructSchema structSchema) {
            //TODO: recurse into Message here
            return structSchema.name();
        }
        if (schema instanceof UnionSchema unionSchema) {
            //TODO: recurse into Union here
        }
        return switch (schema.type()) {
            case BOOLEAN -> "boolean";
            case BYTE -> "sint32";
            case SHORT -> "sint32";
            case INTEGER -> "sint32";
            case LONG -> "sint64";
            case DOUBLE -> "double";
            case FLOAT -> "float";
            case STRING -> "string";
            case MAP -> null;
            case UNION -> null;
            default -> throw new SchemaException("Can not convert schema type " + schema.type() + " to PROTOBUF type");
        };
    }
}
