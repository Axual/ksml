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

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.mapper.DataObjectMapper;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.mapper.StringDataObjectMapper;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.type.DataType;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Serde that converts between KSML DataString (or compatible types) and Kafka's String serialization.
 * <p>
 * Uses a NativeDataObjectMapper to map native inputs to DataObjects, and a StringDataObjectMapper to
 * map between DataString and Java String at the serde boundary. Type safety is enforced against an
 * expected DataType.
 */
public class StringSerde implements Serde<Object> {
    private final DataType expectedType;
    private final DataObjectMapper<String> stringMapper;
    private final NativeDataObjectMapper nativeMapper;

    /**
     * Constructs a StringSerde for DataString using the given native mapper and a default
     * StringDataObjectMapper.
     *
     * @param nativeMapper mapper used to convert user-provided native types to DataObjects
     */
    public StringSerde(NativeDataObjectMapper nativeMapper) {
        this(nativeMapper, new StringDataObjectMapper(), DataString.DATATYPE);
    }

    /**
     * Constructs a StringSerde with custom mappers and expected type.
     *
     * @param nativeMapper mapper used to convert user-provided native types to DataObjects
     * @param stringMapper mapper used at the serde boundary to convert between String and DataObject
     * @param expectedType the expected DataType to validate against
     */
    public StringSerde(NativeDataObjectMapper nativeMapper, DataObjectMapper<String> stringMapper, DataType expectedType) {
        this.nativeMapper = nativeMapper;
        this.stringMapper = stringMapper;
        this.expectedType = expectedType;
    }

    private final StringSerializer serializer = new StringSerializer();
    private final StringDeserializer deserializer = new StringDeserializer();

    /**
     * Provides a serializer that validates and maps input to String before delegating
     * to Kafka's StringSerializer.
     */
    @Override
    public Serializer<Object> serializer() {
        return (topic, data) -> {
            final var dataObject = nativeMapper.toDataObject(expectedType, data);
            // TODO No longer needed as this check is done in the nativeMapper toDataObject
            if (!expectedType.isAssignableFrom(dataObject).isAssignable()) {
                throw new DataException("Incorrect type passed in: expected=" + expectedType + ", got " + dataObject.type());
            }
            var str = stringMapper.fromDataObject(dataObject);
            return serializer.serialize(topic, str);
        };
    }

    /**
     * Provides a deserializer that first converts from String to a DataObject and validates
     * it matches the expected DataType.
     */
    @Override
    public Deserializer<Object> deserializer() {
        return (topic, data) -> {
            final var str = deserializer.deserialize(topic, data);
            final var dataObject = stringMapper.toDataObject(expectedType, str);
            if (dataObject != null && !expectedType.isAssignableFrom(dataObject).isAssignable()) {
                throw new DataException("Wrong type retrieved from state store: expected " + expectedType + ", got " + dataObject.type());
            }
            return dataObject;
        };
    }
}
