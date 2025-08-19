package io.axual.ksml.data.serde;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
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
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.type.DataType;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * A Serde that maps between native Java objects and KSML DataObjects during serialization/deserialization.
 * <p>
 * It uses a pair of mappers:
 * - one for mapping between serde types and DataObject representations (serdeMapper), used during serialization and
 *   deserialization; and
 * - one for mapping - potentially native - objects coming from Kafka Streams topologies to DataObject representations
 *   (nativeMapper), used only during serialization.
 * <p>
 * Exceptions during (de)serialization are wrapped in DataException with a readable context.
 */
public class DataObjectSerde implements Serde<Object>, Serializer<Object>, Deserializer<Object> {
    private static final String DESERIALIZATION_ERROR_MSG = " message could not be deserialized from topic ";
    private static final String SERIALIZATION_ERROR_MSG = " message could not be serialized to topic ";
    private final String name;
    private final Serializer<Object> serializer;
    private final Deserializer<Object> deserializer;
    private final DataType expectedDataType;
    private final DataObjectMapper<Object> serdeMapper;
    private final DataObjectMapper<Object> nativeMapper;

    /**
     * Creates a DataObjectSerde.
     *
     * @param name              a short name used in exception messages
     * @param serializer        the underlying serializer to delegate to
     * @param deserializer      the underlying deserializer to delegate to
     * @param expectedDataType  the expected DataType of values
     * @param serdeMapper       mapper between serde/native types and DataObjects used for serde boundary
     * @param nativeMapper      mapper between user-provided native types and DataObjects
     */
    public DataObjectSerde(String name, Serializer<Object> serializer, Deserializer<Object> deserializer, DataType expectedDataType, DataObjectMapper<Object> serdeMapper, DataObjectMapper<Object> nativeMapper) {
        this.name = name.toUpperCase();
        this.serializer = serializer;
        this.deserializer = deserializer;
        this.expectedDataType = expectedDataType;
        this.serdeMapper = serdeMapper;
        this.nativeMapper = nativeMapper;
    }

    /**
     * Returns this instance as a Serializer to participate in Kafka's Serde contract.
     *
     * @return this instance as Serializer
     */
    public Serializer<Object> serializer() {
        return this;
    }

    /**
     * Returns this instance as a Deserializer to participate in Kafka's Serde contract.
     *
     * @return this instance as Deserializer
     */
    public Deserializer<Object> deserializer() {
        return this;
    }

    /**
     * Configures both the underlying serializer and deserializer with the same settings.
     *
     * @param configs configuration map
     * @param isKey   whether this Serde is used for record keys
     */
    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        serializer.configure(configs, isKey);
        deserializer.configure(configs, isKey);
    }

    /**
     * Deserializes bytes and maps the result into a DataObject of the expected type.
     *
     * @param topic the topic name
     * @param data  serialized bytes, may be null
     * @return the deserialized value as DataObject
     * @throws io.axual.ksml.data.exception.DataException if mapping or deserialization fails
     */
    @Override
    public Object deserialize(final String topic, final byte[] data) {
        try {
            return serdeMapper.toDataObject(expectedDataType, deserializer.deserialize(topic, data));
        } catch (Exception e) {
            throw new DataException(name + DESERIALIZATION_ERROR_MSG + topic, e);
        }
    }

    /**
     * Deserializes bytes with headers and maps the result into a DataObject of the expected type.
     *
     * @param topic   the topic name
     * @param headers the record headers
     * @param data    serialized bytes, may be null
     * @return the deserialized value as DataObject
     * @throws io.axual.ksml.data.exception.DataException if mapping or deserialization fails
     */
    @Override
    public Object deserialize(final String topic, final Headers headers, final byte[] data) {
        try {
            return serdeMapper.toDataObject(expectedDataType, deserializer.deserialize(topic, headers, data));
        } catch (Exception e) {
            throw new DataException(name + DESERIALIZATION_ERROR_MSG + topic, e);
        }
    }

    /**
     * Deserializes from a ByteBuffer with headers and maps the result into a DataObject of the expected type.
     *
     * @param topic   the topic name
     * @param headers the record headers
     * @param data    ByteBuffer containing serialized bytes
     * @return the deserialized value as DataObject
     * @throws io.axual.ksml.data.exception.DataException if mapping or deserialization fails
     */
    @Override
    public Object deserialize(final String topic, final Headers headers, final ByteBuffer data) {
        try {
            return serdeMapper.toDataObject(expectedDataType, deserializer.deserialize(topic, headers, data));
        } catch (Exception e) {
            throw new DataException(name + DESERIALIZATION_ERROR_MSG + topic, e);
        }
    }

    /**
     * Maps the provided value from native form to a DataObject and delegates to the underlying serializer.
     *
     * @param topic the topic name
     * @param data  the value to serialize, may be null
     * @return the serialized bytes
     * @throws io.axual.ksml.data.exception.DataException if mapping or serialization fails
     */
    @Override
    public byte[] serialize(final String topic, final Object data) {
        try {
            final var dataObject = nativeMapper.toDataObject(data);
            if (dataObject == DataNull.INSTANCE) return serializer.serialize(topic, null);
            final var serdeObject = serdeMapper.fromDataObject(dataObject);
            return serializer.serialize(topic, serdeObject);
        } catch (Exception e) {
            throw new DataException(name + SERIALIZATION_ERROR_MSG + topic, e);
        }
    }

    /**
     * Maps the provided value from native form to a DataObject and delegates to the underlying serializer
     * including the provided headers.
     *
     * @param topic   the topic name
     * @param headers the record headers to include
     * @param data    the value to serialize, may be null
     * @return the serialized bytes
     * @throws io.axual.ksml.data.exception.DataException if mapping or serialization fails
     */
    @Override
    public byte[] serialize(final String topic, final Headers headers, final Object data) {
        try {
            final var dataObject = nativeMapper.toDataObject(data);
            if (dataObject == DataNull.INSTANCE) return serializer.serialize(topic, null);
            final var serdeObject = serdeMapper.fromDataObject(dataObject);
            return serializer.serialize(topic, headers, serdeObject);
        } catch (Exception e) {
            throw new DataException(name + SERIALIZATION_ERROR_MSG + topic, e);
        }
    }

    /**
     * Closes both the underlying serializer and deserializer.
     */
    @Override
    public void close() {
        serializer.close();
        deserializer.close();
    }
}
