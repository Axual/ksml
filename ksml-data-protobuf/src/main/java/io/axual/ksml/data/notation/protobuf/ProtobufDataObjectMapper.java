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
import com.google.protobuf.InvalidProtocolBufferException;
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

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;

@Slf4j
public class ProtobufDataObjectMapper extends NativeDataObjectMapper {
    private static final DataTypeDataSchemaMapper DATA_TYPE_MAPPER = new DataTypeDataSchemaMapper();
    private final ProtobufFileElementSchemaMapper elementSchemaMapper;
    private final ProtobufFileElementDescriptorMapper descriptorElementMapper;
    private final ConvertUtil convertUtil;
    // Message descriptors of the local schemas that records are read with, by schema instance
    private final Map<StructSchema, Descriptors.Descriptor> localDescriptors = Collections.synchronizedMap(new IdentityHashMap<>());

    public ProtobufDataObjectMapper(ProtobufFileElementDescriptorMapper descriptorElementMapper) {
        this(descriptorElementMapper, new NativeDataObjectMapper(), new DataTypeDataSchemaMapper());
    }

    public ProtobufDataObjectMapper(ProtobufFileElementDescriptorMapper descriptorElementMapper, NativeDataObjectMapper nativeMapper, DataTypeDataSchemaMapper typeDataSchemaMapper) {
        this.descriptorElementMapper = descriptorElementMapper;
        this.elementSchemaMapper = new ProtobufFileElementSchemaMapper(nativeMapper, typeDataSchemaMapper);
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
        final var schema = elementSchemaMapper.toDataSchema(namespace, name, fileElement);

        // Ensure schema compatibility

        final StructSchema resultSchema;
        final Message resultMessage;
        if (expected != null) {
            final var assignable = expected.isAssignableFrom(schema);
            if (assignable.isNotAssignable()) {
                throw new SchemaException("PROTOBUF schema incompatibility: schema=" + schema + ", expected=" + expected);
            }
            resultSchema = expected;
            resultMessage = parseWithLocalSchema(expected, message);
        } else {
            resultSchema = schema;
            resultMessage = message;
        }

        final var result = new DataStruct(resultSchema);

        for (final var field : resultMessage.getAllFields().entrySet()) {
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

    /**
     * Parses the message again with the local schema. Fields that the local schema does not have, at any level,
     * become unknown fields of the message, so they are not shown as fields.
     */
    private Message parseWithLocalSchema(StructSchema localSchema, Message message) {
        final var localDescriptor = localDescriptors.computeIfAbsent(localSchema, this::toMessageDescriptor);
        try {
            return DynamicMessage.newBuilder(localDescriptor).mergeFrom(message.toByteString()).buildPartial();
        } catch (InvalidProtocolBufferException e) {
            throw new DataException("PROTOBUF message can not be read with schema " + localSchema.fullName(), e);
        }
    }

    private Descriptors.Descriptor toMessageDescriptor(StructSchema dataSchema) {
        final var fileElement = elementSchemaMapper.fromDataSchema(dataSchema);
        final var descriptor = descriptorElementMapper.toDescriptor(dataSchema.namespace(), dataSchema.name(), fileElement);
        return descriptor.findMessageTypeByName(dataSchema.name());
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
        final var msgDescriptor = toMessageDescriptor(dataSchema);
        final var msg = DynamicMessage.newBuilder(msgDescriptor);

        setRegularFields(msg, struct, msgDescriptor, dataSchema);
        setOneOfFields(msg, struct, msgDescriptor, dataSchema);

        return msg.build();
    }

    private void setRegularFields(DynamicMessage.Builder msg, DataStruct struct, Descriptors.Descriptor msgDescriptor, StructSchema dataSchema) {
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
    }

    private void setOneOfFields(DynamicMessage.Builder msg, DataStruct struct, Descriptors.Descriptor msgDescriptor, StructSchema dataSchema) {
        // Copy all oneOf fields by assigning it explicitly to the field with right type
        for (final var oneOf : msgDescriptor.getOneofs()) {
            final var fieldName = oneOf.getName();
            final var fieldValue = struct.get(fieldName);
            if (fieldValue != null) {
                assignOneOfField(msg, msgDescriptor, dataSchema, fieldName, fieldValue);
            }
        }
    }

    private void assignOneOfField(DynamicMessage.Builder msg, Descriptors.Descriptor msgDescriptor, StructSchema dataSchema, String fieldName, DataObject fieldValue) {
        final var fieldSchema = dataSchema.field(fieldName).schema();
        if (!(fieldSchema instanceof UnionSchema unionSchema)) {
            throw new SchemaException("PROTOBUF oneOf does not match data field: schema=" + (fieldSchema != null ? fieldSchema.type() : "null"));
        }
        for (final var memberSchema : unionSchema.members()) {
            final var memberType = new DataTypeDataSchemaMapper().fromDataSchema(memberSchema.schema());
            if (memberType.isAssignableFrom(fieldValue).isAssignable()) {
                setMessageFieldValue(msg, msgDescriptor.findFieldByName(memberSchema.name()), fromDataObject(fieldValue));
                return;
            }
        }
        throw new DataException("Value of type " + fieldValue.getClass().getSimpleName()
                + " does not match any branch of PROTOBUF oneOf '" + fieldName + "'");
    }

    // Package-private to allow unit testing. Wraps setField so type mismatches surface with
    // field-level context instead of a generic ClassCastException from inside the library.
    void setMessageFieldValue(DynamicMessage.Builder msg, Descriptors.FieldDescriptor field, Object value) {
        if (field.getType() == Descriptors.FieldDescriptor.Type.ENUM) {
            final var evd = field.getEnumType().findValueByName(value.toString());
            if (evd == null) {
                throw new SchemaException("Value '" + value + "' not found in enum type '" + field.getEnumType().getName() + "'");
            }
            msg.setField(field, evd);
        } else {
            try {
                msg.setField(field, value);
            } catch (RuntimeException e) {
                throw new DataException("Failed to set Protobuf field '" + field.getFullName()
                        + "' (type=" + field.getType() + ") with value of type "
                        + (value != null ? value.getClass().getSimpleName() : "null"), e);
            }
        }
    }
}
