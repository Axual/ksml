package io.axual.ksml.data.notation.jsonschema;

/*-
 * ========================LICENSE_START=================================
 * KSML
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

import com.google.common.collect.ImmutableMap;
import io.apicurio.registry.rest.client.RegistryClient;
import io.apicurio.registry.serde.jsonschema.JsonSchemaKafkaDeserializer;
import io.apicurio.registry.serde.jsonschema.JsonSchemaKafkaSerializer;
import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.notation.json.JsonDataObjectConverter;
import io.axual.ksml.data.notation.json.JsonDataObjectMapper;
import io.axual.ksml.data.notation.json.JsonSchemaLoader;
import io.axual.ksml.data.notation.string.StringNotation;
import io.axual.ksml.data.type.*;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class JsonSchemaNotation extends StringNotation {
    public static final DataType DEFAULT_TYPE = new UnionType(
            new UnionType.MemberType(new StructType()),
            new UnionType.MemberType(new ListType()));

    public enum SerdeType {
        APICURIO,
        CONFLUENT
    }

    private static final String DESERIALIZATION_ERROR_MSG = " message could not be deserialized from topic ";
    private static final String SERIALIZATION_ERROR_MSG = " message could not be serialized to topic ";
    private final SerdeType serdeType;
    private final NativeDataObjectMapper nativeMapper;
    private final Map<String, ?> serdeConfigs;
    private final RegistryClient client;

    public JsonSchemaNotation(String name, SerdeType type, NativeDataObjectMapper nativeMapper, Map<String, ?> configs, RegistryClient client) {
        super(name, ".json", DEFAULT_TYPE, new JsonDataObjectConverter(), new JsonSchemaLoader(), nativeMapper, new JsonDataObjectMapper(false));
        this.serdeType = type;
        this.nativeMapper = nativeMapper;
        this.serdeConfigs = ImmutableMap.copyOf(configs);
        this.client = client;
    }

    @Override
    public DataType defaultType() {
        return DEFAULT_TYPE;
    }

    @Override
    public Serde<Object> serde(DataType type, boolean isKey) {
        // JSON types can either be Map (or Struct), or List, or the union type of both Struct and List
        if (type instanceof MapType || type instanceof ListType || JsonSchemaNotation.DEFAULT_TYPE.isAssignableFrom(type)) {
            final var serde = switch (serdeType) {
                case APICURIO -> new WrappedSerde(
                        name(),
                        client != null ? new JsonSchemaKafkaSerializer<>(client) : new JsonSchemaKafkaSerializer<>(),
                        client != null ? new JsonSchemaKafkaDeserializer<>(client) : new JsonSchemaKafkaDeserializer<>(),
                        nativeMapper);
                case CONFLUENT -> new WrappedSerde(
                        name(),
                        new KafkaJsonSchemaSerializer<>(),
                        new KafkaJsonSchemaDeserializer<>(),
                        nativeMapper);
            };
            serde.configure(serdeConfigs, isKey);
            return serde;
        }
        // Other types cannot be serialized as JSON
        throw noSerdeFor(type);
    }

    private record WrappedSerde(String name, Serializer<Object> serializer, Deserializer<Object> deserializer,
                                NativeDataObjectMapper nativeMapper) implements Serializer<Object>, Deserializer<Object>, Serde<Object> {
        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {
            serializer.configure(configs, isKey);
            deserializer.configure(configs, isKey);
        }

        @Override
        public Object deserialize(final String topic, final byte[] data) {
            try {
                return nativeMapper.toDataObject(deserializer.deserialize(topic, data));
            } catch (Exception e) {
                throw new DataException(name.toUpperCase() + DESERIALIZATION_ERROR_MSG + topic, e);
            }
        }

        @Override
        public Object deserialize(final String topic, final Headers headers, final byte[] data) {
            try {
                return nativeMapper.toDataObject(deserializer.deserialize(topic, headers, data));
            } catch (Exception e) {
                throw new DataException(name.toUpperCase() + DESERIALIZATION_ERROR_MSG + topic, e);
            }
        }

        @Override
        public Object deserialize(final String topic, final Headers headers, final ByteBuffer data) {
            try {
                return nativeMapper.toDataObject(deserializer.deserialize(topic, headers, data));
            } catch (Exception e) {
                throw new DataException(name.toUpperCase() + DESERIALIZATION_ERROR_MSG + topic, e);
            }
        }

        @Override
        public byte[] serialize(final String topic, final Object data) {
            try {
                return serializer.serialize(topic, nativeMapper.fromDataObject(nativeMapper.toDataObject(data)));
            } catch (Exception e) {
                throw new DataException(name + SERIALIZATION_ERROR_MSG + topic, e);
            }
        }

        @Override
        public byte[] serialize(final String topic, final Headers headers, final Object data) {
            try {
                return serializer.serialize(topic, headers, nativeMapper.fromDataObject(nativeMapper.toDataObject(data)));
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
}
