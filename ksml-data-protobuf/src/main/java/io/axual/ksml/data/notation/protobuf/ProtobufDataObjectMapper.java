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
import com.squareup.wire.Syntax;
import com.squareup.wire.schema.Location;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import io.apicurio.registry.utils.protobuf.schema.ProtobufSchema;
import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.mapper.DataObjectMapper;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.type.DataType;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;

@Slf4j
public class ProtobufDataObjectMapper implements DataObjectMapper<Message> {
    private static final ProtobufSchemaMapper PROTOBUF_SCHEMA_MAPPER = new ProtobufSchemaMapper();
    private static final NativeDataObjectMapper NATIVE_DATA_MAPPER = new NativeDataObjectMapper();

    @Override
    public DataObject toDataObject(DataType expected, Message value) {
        if (value == null) return NativeDataObjectMapper.convertFromNull(expected);
        final var descriptor = value.getDescriptorForType();
        final var namespace = descriptor.getFile().getPackage();
        final var name = descriptor.getName();
        final var fileElem = new ProtoFileElement(
                Location.get(""), namespace,
                Syntax.valueOf(descriptor.getFile().toProto().getSyntax()),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList());
        final var schema = PROTOBUF_SCHEMA_MAPPER.toDataSchema(namespace, name, new ProtobufSchema(descriptor.getFile(), fileElem));

        final var result = new DataStruct(schema);
        for (final var field : value.getAllFields().entrySet()) {
            result.put(field.getKey().getName(), NATIVE_DATA_MAPPER.toDataObject(field.getValue()));
        }
        return new DataStruct(schema);
    }

    @Override
    public Message fromDataObject(DataObject value) {
        if (value instanceof DataNull) return null;
        if (value instanceof DataStruct struct) {
            final var dataSchema = struct.type().schema();
            if (dataSchema != null) {
                final var protoSchema = PROTOBUF_SCHEMA_MAPPER.fromDataSchema(dataSchema);
                final var msgSchema = protoSchema.getFileDescriptor().findMessageTypeByName(dataSchema.name());
                final var msg = DynamicMessage.newBuilder(protoSchema.getFileDescriptor().findMessageTypeByName(dataSchema.name()));
                for (final var field : msgSchema.getFields()) {
                    final var fieldValue = struct.get(field.getName());
                    if (fieldValue != null) {
                        final var nativeValue = NATIVE_DATA_MAPPER.fromDataObject(struct.get(field.getName()));
                        if (field.getType() == Descriptors.FieldDescriptor.Type.ENUM) {
                            final var evd = field.getEnumType().findValueByName(nativeValue.toString());
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
        return null;
    }
}
