package io.axual.ksml.serde;

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

import io.axual.ksml.data.mapper.DataObjectMapper;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.schema.DataField;
import io.axual.ksml.data.mapper.DataTypeSchemaMapper;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.execution.FatalError;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import lombok.Getter;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.List;

@Getter
public class DataObjectDeserializer implements Deserializer<Object> {
    private static final DataObjectMapper NATIVE_MAPPER = new NativeDataObjectMapper();
    private static final String FIELD_NAME = "data";
    private final DataType expectedType;
    private final DataType wrapperType;
    private final Deserializer<Object> jsonDeserializer;

    public DataObjectDeserializer(DataType type) {
        expectedType = type;
        final var dataObjectSchema = DataTypeSchemaMapper.dataTypeToSchema(expectedType);
        final var wrapperField = new DataField(FIELD_NAME, dataObjectSchema, "");
        final var wrapperSchema = new StructSchema("io.axual.ksml.data", "DataObject", "", List.of(wrapperField));
        wrapperType = DataTypeSchemaMapper.schemaToDataType(wrapperSchema);
        try (final var serde = new JsonSerde(wrapperType)) {
            jsonDeserializer = serde.deserializer();
        }
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        final var wrapper = jsonDeserializer.deserialize(topic, data);
        if (wrapper == null) {
            throw FatalError.executionError("Retrieved unexpected null wrapper from state store " + topic);
        }
        if (!(wrapper instanceof DataStruct wrapperStruct) || !wrapperType.isAssignableFrom(wrapperType)) {
            throw FatalError.executionError("Incorrect type deserialized from " + topic + ": " + wrapper);
        }

        final var result = wrapperStruct.get(FIELD_NAME);
        if (result != null && !expectedType.isAssignableFrom(result)) {
            throw FatalError.executionError("Wrong type retrieved from state store: expected " + expectedType + ", got " + result.type());
        }
        // For compatibility reasons, we have to return native object format here. Otherwise, state stores assisting in
        // count() operations do not get back expected Long types, for example.
        return NATIVE_MAPPER.fromDataObject(result);
    }
}
