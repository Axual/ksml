package io.axual.ksml.data.serde;

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

import io.axual.ksml.data.exception.ExecutionException;
import io.axual.ksml.data.mapper.DataSchemaMapper;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.schema.DataField;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.DataType;
import lombok.Getter;
import org.apache.kafka.common.serialization.Serializer;

import java.util.List;

import static io.axual.ksml.data.parser.schema.DataSchemaDSL.DATA_OBJECT_TYPE_NAME;
import static io.axual.ksml.data.parser.schema.DataSchemaDSL.DATA_SCHEMA_NAMESPACE;

@Getter
public class DataObjectSerializer implements Serializer<DataObject> {
    private static final String FIELD_NAME = "data";
    private final DataType expectedType;
    private final StructSchema wrapperSchema;
    private final DataType wrapperType;
    private final Serializer<Object> jsonSerializer;

    public DataObjectSerializer(DataType type, DataSchemaMapper<DataType> dataTypeSchemaMapper) {
        this.expectedType = type;
        final var dataObjectSchema = dataTypeSchemaMapper.toDataSchema(expectedType);
        final var wrapperField = new DataField(FIELD_NAME, dataObjectSchema, "");
        wrapperSchema = new StructSchema(DATA_SCHEMA_NAMESPACE, DATA_OBJECT_TYPE_NAME, "", List.of(wrapperField));
        wrapperType = dataTypeSchemaMapper.fromDataSchema(wrapperSchema);
        try (final var jsonSerde = new JsonSerde(new NativeDataObjectMapper(), wrapperType)) {
            jsonSerializer = jsonSerde.serializer();
        }
    }

    @Override
    public byte[] serialize(String topic, DataObject data) {
        if (data == null || data == DataNull.INSTANCE) return null;
        if (!expectedType.isAssignableFrom(data.type())) {
            throw new ExecutionException("Incorrect type passed in: expected=" + expectedType + ", got " + data.type());
        }
        final var wrapper = new DataStruct(wrapperSchema);
        wrapper.put(FIELD_NAME, data);
        return jsonSerializer.serialize(topic, wrapper);
    }
}
