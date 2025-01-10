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

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.exception.SchemaException;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.type.DataType;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProtobufDataObjectMapper extends NativeDataObjectMapper {
    private static final ProtobufDescriptorFileElementMapper DESCRIPTOR_ELEMENT_MAPPER = new ProtobufDescriptorFileElementMapper();
    private static final ProtobufFileElementSchemaMapper ELEMENT_SCHEMA_MAPPER = new ProtobufFileElementSchemaMapper();

    @Override
    public DataObject toDataObject(DataType expected, Object value) {
        if (value instanceof Message message) return convertFromMessage(message);
        return super.toDataObject(expected, value);
    }

    private DataObject convertFromMessage(Message message) {
        final var descriptor = message.getDescriptorForType();
        final var namespace = descriptor.getFile().getPackage();
        final var name = descriptor.getName();
        final var fileElement = DESCRIPTOR_ELEMENT_MAPPER.toFileElement(descriptor);
        final var schema = ELEMENT_SCHEMA_MAPPER.toDataSchema(namespace, name, fileElement);
        final var result = new DataStruct(schema);
        for (final var field : message.getAllFields().entrySet()) {
            var val = field.getValue();
            if (val instanceof Descriptors.EnumValueDescriptor enumValue) val = enumValue.getName();
            result.put(field.getKey().getName(), toDataObject(val));
        }
        return result;
    }

    @Override
    public Object fromDataObject(DataObject value) {
        if (value instanceof DataStruct struct) {
            return convertToMessage(struct);
        }
        return super.fromDataObject(value);
    }

    private Message convertToMessage(DataStruct struct) {
        final var dataSchema = struct.type().schema();
        if (dataSchema == null) {
            throw new DataException("Can not convert schemaless STRUCT into a PROTOBUF message");
        }
        final var fileElement = ELEMENT_SCHEMA_MAPPER.fromDataSchema(dataSchema);
        final var descriptor = DESCRIPTOR_ELEMENT_MAPPER.toDescriptor(dataSchema.namespace(), dataSchema.name(), fileElement);
        final var msgDescriptor = descriptor.findMessageTypeByName(dataSchema.name());
        final var msg = DynamicMessage.newBuilder(msgDescriptor);
        for (final var field : msgDescriptor.getFields()) {
            final var fieldValue = struct.get(field.getName());
            if (fieldValue != null) {
                final var nativeValue = fromDataObject(struct.get(field.getName()));
                if (field.getType() == Descriptors.FieldDescriptor.Type.ENUM) {
                    final var evd = field.getEnumType().findValueByName(nativeValue.toString());
                    if (evd == null) {
                        throw new SchemaException("Value '" + nativeValue + "' not found in enum type '" + field.getEnumType().getName() + "'");
                    }
                    msg.setField(field, evd);
                } else {
                    msg.setField(field, nativeValue);
                }
            } else {
                if (field.isRequired()) {
                    throw new DataException("PROTOBUF message of type '" + dataSchema.name() + "' is missing required field '" + field.getName() + "'");
                }
            }
        }
        return msg.build();
    }
}

