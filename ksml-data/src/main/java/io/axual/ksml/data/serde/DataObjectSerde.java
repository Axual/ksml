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
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class DataObjectSerde implements Serde<Object>, Serializer<Object>, Deserializer<Object> {
    private static final String DESERIALIZATION_ERROR_MSG = " message could not be deserialized from topic ";
    private static final String SERIALIZATION_ERROR_MSG = " message could not be serialized to topic ";
    private final String name;
    private final Serializer<Object> serializer;
    private final Deserializer<Object> deserializer;
    private final DataObjectMapper<Object> serdeMapper;
    private final DataObjectMapper<Object> nativeMapper;

    public DataObjectSerde(String name, Serializer<Object> serializer, Deserializer<Object> deserializer, DataObjectMapper<Object> serdeMapper, DataObjectMapper<Object> nativeMapper) {
        this.name = name;
        this.serializer = serializer;
        this.deserializer = deserializer;
        this.serdeMapper = serdeMapper;
        this.nativeMapper = nativeMapper;
    }

    public Serializer<Object> serializer() {
        return this;
    }

    public Deserializer<Object> deserializer() {
        return this;
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        serializer.configure(configs, isKey);
        deserializer.configure(configs, isKey);
    }

    @Override
    public Object deserialize(final String topic, final byte[] data) {
        try {
            return serdeMapper.toDataObject(deserializer.deserialize(topic, data));
        } catch (Exception e) {
            throw new DataException(name.toUpperCase() + DESERIALIZATION_ERROR_MSG + topic, e);
        }
    }

    @Override
    public Object deserialize(final String topic, final Headers headers, final byte[] data) {
        try {
            return serdeMapper.toDataObject(deserializer.deserialize(topic, headers, data));
        } catch (Exception e) {
            throw new DataException(name.toUpperCase() + DESERIALIZATION_ERROR_MSG + topic, e);
        }
    }

    @Override
    public Object deserialize(final String topic, final Headers headers, final ByteBuffer data) {
        try {
            return serdeMapper.toDataObject(deserializer.deserialize(topic, headers, data));
        } catch (Exception e) {
            throw new DataException(name.toUpperCase() + DESERIALIZATION_ERROR_MSG + topic, e);
        }
    }

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

    @Override
    public void close() {
        serializer.close();
        deserializer.close();
    }
}
