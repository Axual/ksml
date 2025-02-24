package io.axual.ksml.data.notation.avro;

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
import io.apicurio.registry.serde.avro.AvroKafkaDeserializer;
import io.apicurio.registry.serde.avro.AvroKafkaSerializer;
import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.notation.BaseNotation;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.MapType;
import io.axual.ksml.data.type.StructType;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.Getter;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class AvroNotation extends BaseNotation {
    public static final DataType DEFAULT_TYPE = new StructType();

    public enum SerdeType {
        APICURIO,
        CONFLUENT
    }

    private static final AvroDataObjectMapper AVRO_MAPPER = new AvroDataObjectMapper();
    private static final String DESERIALIZATION_ERROR_MSG = " message could not be deserialized from topic ";
    private static final String SERIALIZATION_ERROR_MSG = " message could not be serialized to topic ";
    private final SerdeType serdeType;
    private final NativeDataObjectMapper nativeMapper;
    private final Map<String, ?> serdeConfigs;
    private Serde<Object> keySerde;
    private Serde<Object> valueSerde;

    public AvroNotation(String name, SerdeType type, NativeDataObjectMapper nativeMapper, Map<String, ?> configs) {
        super(name, ".avsc", DEFAULT_TYPE, null, new AvroSchemaParser());
        this.serdeType = type;
        this.nativeMapper = nativeMapper;
        this.serdeConfigs = ImmutableMap.copyOf(configs);
    }

    @Override
    public Serde<Object> serde(DataType type, boolean isKey) {
        if (!(type instanceof MapType)) throw noSerdeFor(type);

        // Create the serdes only upon request to prevent error messages on missing SR url configs if AVRO is not used
        if (keySerde == null) {
            keySerde = new AvroSerde(serdeType);
            keySerde.configure(serdeConfigs, true);
        }
        if (valueSerde == null) {
            valueSerde = new AvroSerde(serdeType);
            valueSerde.configure(serdeConfigs, false);
        }
        return isKey ? keySerde : valueSerde;
    }

    private class AvroSerde implements Serde<Object> {
        private final Serializer<Object> backingSerializer;
        private final Deserializer<Object> backingDeserializer;
        @Getter
        private final Serializer<Object> serializer;
        @Getter
        private final Deserializer<Object> deserializer;

        public AvroSerde(SerdeType type) {
            backingSerializer = switch (type) {
                case APICURIO -> new AvroKafkaSerializer<>();
                case CONFLUENT -> new KafkaAvroSerializer();
            };
            backingDeserializer = switch (type) {
                case APICURIO -> new AvroKafkaDeserializer<>();
                case CONFLUENT -> new KafkaAvroDeserializer();
            };

            final var wrappedSerde = new WrappedSerde(name(), backingSerializer, backingDeserializer, nativeMapper);
            serializer = wrappedSerde;
            deserializer = wrappedSerde;
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            backingSerializer.configure(configs, isKey);
            backingDeserializer.configure(configs, isKey);
        }
    }

    private record WrappedSerde(String name, Serializer<Object> serializer, Deserializer<Object> deserializer,
                                NativeDataObjectMapper nativeMapper) implements Serializer<Object>, Deserializer<Object> {
        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {
            serializer.configure(configs, isKey);
            deserializer.configure(configs, isKey);
        }

        @Override
        public Object deserialize(final String topic, final byte[] data) {
            try {
                return AVRO_MAPPER.toDataObject(deserializer.deserialize(topic, data));
            } catch (Exception e) {
                throw new DataException(name + DESERIALIZATION_ERROR_MSG + topic, e);
            }
        }

        @Override
        public Object deserialize(final String topic, final Headers headers, final byte[] data) {
            try {
                return AVRO_MAPPER.toDataObject(deserializer.deserialize(topic, headers, data));
            } catch (Exception e) {
                throw new DataException(name + DESERIALIZATION_ERROR_MSG + topic, e);
            }
        }

        @Override
        public Object deserialize(final String topic, final Headers headers, final ByteBuffer data) {
            try {
                return AVRO_MAPPER.toDataObject(deserializer.deserialize(topic, headers, data));
            } catch (Exception e) {
                throw new DataException(name + DESERIALIZATION_ERROR_MSG + topic, e);
            }
        }

        @Override
        public byte[] serialize(final String topic, final Object data) {
            try {
                return serializer.serialize(topic, AVRO_MAPPER.fromDataObject(nativeMapper.toDataObject(data)));
            } catch (Exception e) {
                throw new DataException(name + SERIALIZATION_ERROR_MSG + topic, e);
            }
        }

        @Override
        public byte[] serialize(final String topic, final Headers headers, final Object data) {
            try {
                return serializer.serialize(topic, headers, AVRO_MAPPER.fromDataObject(nativeMapper.toDataObject(data)));
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
