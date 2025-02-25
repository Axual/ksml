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

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;
import io.apicurio.registry.rest.client.RegistryClient;
import io.apicurio.registry.serde.protobuf.ProtobufKafkaDeserializer;
import io.apicurio.registry.serde.protobuf.ProtobufKafkaSerializer;
import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.notation.BaseNotation;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.MapType;
import io.axual.ksml.data.type.StructType;
import lombok.Getter;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class ProtobufNotation extends BaseNotation {
    public static final DataType DEFAULT_TYPE = new StructType();

    public enum SerdeType {
        APICURIO,
    }

    private static final String DESERIALIZATION_ERROR_MSG = "Error deserializing Protobuf message from topic ";
    private static final String SERIALIZATION_ERROR_MSG = "Error serializing Protobuf message to topic ";
    private static final ProtobufDataObjectMapper MAPPER = new ProtobufDataObjectMapper();
    private final SerdeType serdeType;
    private final NativeDataObjectMapper nativeMapper;
    private final Map<String, ?> serdeConfigs;
    private final RegistryClient client;

    public ProtobufNotation(String name, SerdeType type, NativeDataObjectMapper nativeMapper, Map<String, ?> configs) {
        this(name, type, nativeMapper, configs, null);
    }

    public ProtobufNotation(String name, SerdeType type, NativeDataObjectMapper nativeMapper, Map<String, ?> configs, RegistryClient client) {
        super(name, ".proto", DEFAULT_TYPE, null, new ProtobufSchemaParser());
        this.serdeType = type;
        this.nativeMapper = nativeMapper;
        this.serdeConfigs = ImmutableMap.copyOf(configs);
        this.client = client;
    }

    @Override
    public Serde<Object> serde(DataType type, boolean isKey) {
        if (!(type instanceof MapType)) throw new DataException("PROTOBUF serde can not be applied to type " + type);

        // Create the serdes only upon request to prevent error messages on missing SR url configs if Protobuf is not used
        final var serde = new ProtobufSerde(serdeType, client);
        serde.configure(serdeConfigs, isKey);
        return serde;
    }

    private class ProtobufSerde implements Serde<Object> {
        private final Serializer<Message> backingSerializer;
        private final Deserializer<Message> backingDeserializer;
        @Getter
        private final Serializer<Object> serializer;
        @Getter
        private final Deserializer<Object> deserializer;

        public ProtobufSerde(SerdeType type, RegistryClient client) {
            backingSerializer = switch (type) {
                case APICURIO ->
                        client != null ? new ProtobufKafkaSerializer<>(client) : new ProtobufKafkaSerializer<>();
            };
            backingDeserializer = switch (type) {
                case APICURIO ->
                        client != null ? new ProtobufKafkaDeserializer<>(client) : new ProtobufKafkaDeserializer<>();
            };

            var wrappedSerde = new WrappedSerde(backingSerializer, backingDeserializer, nativeMapper);
            serializer = wrappedSerde;
            deserializer = wrappedSerde;
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            backingSerializer.configure(configs, isKey);
            backingDeserializer.configure(configs, isKey);
        }
    }

    private record WrappedSerde(Serializer<Message> serializer, Deserializer<Message> deserializer,
                                NativeDataObjectMapper nativeMapper) implements Serializer<Object>, Deserializer<Object> {
        @Override
        public DataObject deserialize(final String topic, final byte[] data) {
            try {
                return MAPPER.toDataObject(deserializer.deserialize(topic, data));
            } catch (Exception e) {
                throw new DataException(DESERIALIZATION_ERROR_MSG + topic, e);
            }
        }

        @Override
        public DataObject deserialize(final String topic, final Headers headers, final byte[] data) {
            try {
                return MAPPER.toDataObject(deserializer.deserialize(topic, headers, data));
            } catch (Exception e) {
                throw new DataException(DESERIALIZATION_ERROR_MSG + topic, e);
            }
        }

        @Override
        public DataObject deserialize(final String topic, final Headers headers, final ByteBuffer data) {
            try {
                return MAPPER.toDataObject(deserializer.deserialize(topic, headers, data));
            } catch (Exception e) {
                throw new DataException(DESERIALIZATION_ERROR_MSG + topic, e);
            }
        }

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {
            serializer.configure(configs, isKey);
            deserializer.configure(configs, isKey);
        }

        @Override
        public byte[] serialize(final String topic, final Object data) {
            try {
                final var dataObject = nativeMapper.toDataObject(data);
                if (dataObject == DataNull.INSTANCE) return serializer.serialize(topic, null);
                final var message = MAPPER.fromDataObject(dataObject);
                if (message instanceof Message msg) return serializer.serialize(topic, msg);
                throw new DataException("Could not convert '" + dataObject.type() + "' to PROTOBUF message");
            } catch (Exception e) {
                throw new DataException(SERIALIZATION_ERROR_MSG + topic, e);
            }
        }

        @Override
        public byte[] serialize(final String topic, final Headers headers, final Object data) {
            try {
                if (data == null) return serializer.serialize(topic, headers, null);
                final var dataObject = nativeMapper.toDataObject(data);
                final var message = MAPPER.fromDataObject(dataObject);
                if (message instanceof Message msg) return serializer.serialize(topic, headers, msg);
                throw new DataException("Could not convert '" + dataObject.type() + "' to PROTOBUF message");
            } catch (Exception e) {
                throw new DataException(SERIALIZATION_ERROR_MSG + topic, e);
            }
        }

        @Override
        public void close() {
            serializer.close();
            deserializer.close();
        }
    }
}
