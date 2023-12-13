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

import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.schema.DataField;
import io.axual.ksml.data.schema.SchemaUtil;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.util.DataUtil;
import lombok.Getter;
import org.apache.kafka.common.serialization.Serializer;

import java.util.List;

@Getter
public class DataObjectSerializer implements Serializer<Object> {
    private static final String FIELD_NAME = "data";
    private final DataType expectedType;
    private final StructSchema wrapperSchema;
    private final DataType wrapperType;
    private final Serializer<Object> jsonSerializer;

    public DataObjectSerializer(DataType type) {
        expectedType = type;
        final var dataObjectSchema = SchemaUtil.dataTypeToSchema(expectedType);
        final var wrapperField = new DataField(FIELD_NAME, dataObjectSchema, "");
        wrapperSchema = new StructSchema("io.axual.ksml.data", "DataObject", "", List.of(wrapperField));
        wrapperType = SchemaUtil.schemaToDataType(wrapperSchema);
        try (final var jsonSerde = new JsonSerde(wrapperType)) {
            jsonSerializer = jsonSerde.serializer();
        }
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        final var dataObject = DataUtil.asDataObject(data);
        if (!expectedType.isAssignableFrom(dataObject.type())) {
            throw new KSMLExecutionException("Incorrect type passed in: expected=" + expectedType + ", got " + dataObject.type());
        }
        final var wrapper = new DataStruct(wrapperSchema);
        wrapper.put(FIELD_NAME, dataObject);
        return jsonSerializer.serialize(topic, wrapper);
    }
}
