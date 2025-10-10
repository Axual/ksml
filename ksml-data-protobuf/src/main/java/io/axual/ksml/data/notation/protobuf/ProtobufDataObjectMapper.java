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
import io.axual.ksml.data.mapper.DataTypeDataSchemaMapper;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.schema.UnionSchema;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.data.util.ConvertUtil;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProtobufDataObjectMapper extends NativeDataObjectMapper {
    private static final DataTypeDataSchemaMapper DATA_TYPE_MAPPER = new DataTypeDataSchemaMapper();
    private static final ProtobufFileElementSchemaMapper ELEMENT_SCHEMA_MAPPER = new ProtobufFileElementSchemaMapper();
    private final ProtobufDescriptorFileElementMapper descriptorElementMapper;
    private final ConvertUtil convertUtil;

    public ProtobufDataObjectMapper(ProtobufDescriptorFileElementMapper descriptorElementMapper) {
        this.descriptorElementMapper = descriptorElementMapper;
        convertUtil = new ConvertUtil(this, DATA_TYPE_MAPPER);
    }

    @Override
    public DataObject toDataObject(DataType expected, Object value) {
        if (value instanceof Message message) {
            final var expectedSchema = expected instanceof StructType expectedStructType ? expectedStructType.schema() : null;
            return convertMessageToDataObject(expectedSchema, message);
        }
        return super.toDataObject(expected, value);
    }

    private DataObject convertMessageToDataObject(StructSchema expected, Message message) {
        final var descriptor = message.getDescriptorForType();
        final var namespace = descriptor.getFile().getPackage();
        final var name = descriptor.getName();
        final var fileElement = descriptorElementMapper.toFileElement(descriptor);
        final var schema = ELEMENT_SCHEMA_MAPPER.toDataSchema(namespace, name, fileElement);

        // Ensure schema compatibility

        final StructSchema resultSchema;
        if (expected != null) {
            final var assignable = expected.isAssignableFrom(schema);
            if (assignable.isNotAssignable()) {
                throw new SchemaException("PROTOBUF schema incompatibility: schema=" + schema + ", expected=" + expected);
            }
            resultSchema = expected;
        } else {
            resultSchema = schema;
        }

        final var result = new DataStruct(resultSchema);

        for (final var field : message.getAllFields().entrySet()) {
            var val = field.getValue();
            if (val instanceof Descriptors.EnumValueDescriptor enumValue) val = enumValue.getName();
            final var parentOneOf = field.getKey().getContainingOneof();
            final var fieldName = parentOneOf != null ? parentOneOf.getName() : field.getKey().getName();
            final var expectedType = DATA_TYPE_MAPPER.fromDataSchema(resultSchema.field(fieldName).schema());
            final var dataObject = convertUtil.convert(null, null, expectedType, toDataObject(val), false);
            result.put(fieldName, dataObject);
        }
        return result;
    }

    @Override
    public Object fromDataObject(DataObject value) {
        if (value instanceof DataStruct struct && !struct.isNull()) {
            return convertDataStructToMessage(struct);
        }
        return super.fromDataObject(value);
    }

    private Message convertDataStructToMessage(DataStruct struct) {
        final var dataSchema = struct.type().schema();
        if (dataSchema == null) {
            throw new DataException("Can not convert schemaless STRUCT into a PROTOBUF message");
        }
        final var fileElement = ELEMENT_SCHEMA_MAPPER.fromDataSchema(dataSchema);
        final var descriptor = descriptorElementMapper.toDescriptor(dataSchema.namespace(), dataSchema.name(), fileElement);
        final var msgDescriptor = descriptor.findMessageTypeByName(dataSchema.name());
        final var msg = DynamicMessage.newBuilder(msgDescriptor);

        // Copy all regular field values (ie. not part of a oneOf)
        for (final var field : msgDescriptor.getFields()) {
            final var parentOneOf = field.getContainingOneof();
            if (parentOneOf == null) {
                final var fieldValue = struct.get(field.getName());
                if (fieldValue != null) {
                    setMessageFieldValue(msg, field, fromDataObject(fieldValue));
                } else {
                    if (field.isRequired()) {
                        throw new DataException("PROTOBUF message of type '" + dataSchema.name() + "' is missing required field '" + field.getName() + "'");
                    }
                }
            }
        }

        // Copy all oneOf fields by assigning it explicitly to the field with right type
        for (final var oneOf : msgDescriptor.getOneofs()) {
            final var fieldName = oneOf.getName();
            final var fieldValue = struct.get(fieldName);
            if (fieldValue != null) {
                final var fieldSchema = dataSchema.field(fieldName).schema();
                if (fieldSchema instanceof UnionSchema unionSchema) {
                    var assigned = false;
                    var index = 0;
                    while (!assigned && index < unionSchema.members().length) {
                        final var memberSchema = unionSchema.members()[index];
                        final var memberType = new DataTypeDataSchemaMapper().fromDataSchema(memberSchema.schema());
                        if (memberType.isAssignableFrom(fieldValue).isAssignable()) {
                            setMessageFieldValue(msg, msgDescriptor.findFieldByName(memberSchema.name()), fromDataObject(fieldValue));
                            assigned = true;
                        }
                        index++;
                    }
                } else {
                    throw new SchemaException("PROTOBUF oneOf does not match data field: schema=" + (fieldSchema != null ? fieldSchema.type() : "null"));
                }
            }
        }

        return msg.build();
    }

    private void setMessageFieldValue(DynamicMessage.Builder msg, Descriptors.FieldDescriptor field, Object value) {
        if (field.getType() == Descriptors.FieldDescriptor.Type.ENUM) {
            final var evd = field.getEnumType().findValueByName(value.toString());
            if (evd == null) {
                throw new SchemaException("Value '" + value + "' not found in enum type '" + field.getEnumType().getName() + "'");
            }
            msg.setField(field, evd);
        } else {
            msg.setField(field, value);
        }
    }
}
