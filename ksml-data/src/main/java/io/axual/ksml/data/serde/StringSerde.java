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
import io.axual.ksml.data.mapper.DataObjectMapper;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.type.DataType;
import org.apache.kafka.common.serialization.*;

public class StringSerde implements Serde<Object> {
    private final DataType expectedType;
    private final DataObjectMapper<String> stringMapper;
    private final NativeDataObjectMapper nativeMapper;

    public StringSerde(DataObjectMapper<String> stringMapper, DataType expectedType) {
        this.stringMapper = stringMapper;
        this.expectedType = expectedType;
        this.nativeMapper = NativeDataObjectMapper.SUPPLIER().create();
    }

    private final StringSerializer serializer = new StringSerializer();
    private final StringDeserializer deserializer = new StringDeserializer();

    @Override
    public Serializer<Object> serializer() {
        return (topic, data) -> {
            final var dataObject = nativeMapper.toDataObject(data);
            if (!expectedType.isAssignableFrom(dataObject)) {
                throw new ExecutionException("Incorrect type passed in: expected=" + expectedType + ", got " + dataObject.type());
            }
            var str = stringMapper.fromDataObject(dataObject);
            return serializer.serialize(topic, str);
        };
    }

    @Override
    public Deserializer<Object> deserializer() {
        return (topic, data) -> {
            final var str = deserializer.deserialize(topic, data);
            final var dataObject = stringMapper.toDataObject(expectedType, str);
            if (dataObject != null && !expectedType.isAssignableFrom(dataObject)) {
                throw new ExecutionException("Wrong type retrieved from state store: expected " + expectedType + ", got " + dataObject.type());
            }
            return dataObject;
        };
    }
}
