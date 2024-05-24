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
import io.axual.ksml.data.mapper.DataTypeSchemaMapper;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.schema.DataField;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.DataType;
import lombok.Getter;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.List;

import static io.axual.ksml.data.parser.schema.DataSchemaDSL.DATA_OBJECT_TYPE_NAME;
import static io.axual.ksml.data.parser.schema.DataSchemaDSL.DATA_SCHEMA_NAMESPACE;

@Getter
public class DataObjectDeserializer implements Deserializer<DataObject> {
    private static final String FIELD_NAME = "data";
    private final DataType expectedType;
    private final DataType wrapperType;
    private final Deserializer<Object> jsonDeserializer;

    public DataObjectDeserializer(DataType type, DataTypeSchemaMapper dataTypeDataSchemaMapper) {
        expectedType = type;
        final var dataObjectSchema = dataTypeDataSchemaMapper.toDataSchema(expectedType);
        final var wrapperField = new DataField(FIELD_NAME, dataObjectSchema, "");
        final var wrapperSchema = new StructSchema(DATA_SCHEMA_NAMESPACE, DATA_OBJECT_TYPE_NAME, DATA_OBJECT_TYPE_NAME, List.of(wrapperField));
        wrapperType = dataTypeDataSchemaMapper.fromDataSchema(wrapperSchema);
        try (final var serde = new JsonSerde(wrapperType)) {
            jsonDeserializer = serde.deserializer();
        }
    }

    @Override
    public DataObject deserialize(String topic, byte[] data) {
        if (data == null || data.length == 0) return DataNull.INSTANCE;
        final var wrapper = jsonDeserializer.deserialize(topic, data);
        if (wrapper == null) {
            throw new ExecutionException("Retrieved unexpected null wrapper from state store " + topic);
        }
        if (!(wrapper instanceof DataStruct wrapperStruct) || !wrapperType.isAssignableFrom(wrapperType)) {
            throw new ExecutionException("Incorrect type deserialized from " + topic + ": " + wrapper);
        }

        final var result = wrapperStruct.get(FIELD_NAME);
        if (result != null && !expectedType.isAssignableFrom(result)) {
            throw new ExecutionException("Wrong type retrieved from state store: expected " + expectedType + ", got " + result.type());
        }
        return result;
    }
}
