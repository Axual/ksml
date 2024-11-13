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
import io.axual.ksml.data.exception.ExecutionException;
import io.axual.ksml.data.loader.SchemaLoader;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.notation.Notation;
import io.axual.ksml.data.notation.NotationConverter;
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

public class AvroNotation implements Notation {
    public static final DataType DEFAULT_TYPE = new StructType();

    public enum SerdeType {
        APICURIO,
        CONFLUENT
    }

    private static final AvroDataObjectMapper mapper = new AvroDataObjectMapper();
    private final SerdeType serdeType;
    private final NativeDataObjectMapper nativeMapper;
    private final Map<String, ?> serdeConfigs;
    @Getter
    private final SchemaLoader loader;
    private Serde<Object> keySerde;
    private Serde<Object> valueSerde;

    public AvroNotation(SerdeType type, NativeDataObjectMapper nativeMapper, Map<String, ?> configs, SchemaLoader loader) {
        this.serdeType = type;
        this.nativeMapper = nativeMapper;
        this.serdeConfigs = ImmutableMap.copyOf(configs);
        this.loader = loader;
    }

    @Override
    public DataType defaultType() {
        return DEFAULT_TYPE;
    }

    @Override
    public Serde<Object> serde(DataType type, boolean isKey) {
        if (!(type instanceof MapType)) throw new DataException("AVRO serde can not be applied to type " + type);

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

    public NotationConverter converter() {
        return null;
    }

    private class AvroSerde implements Serde<Object> {
        private final Serializer<Object> serializer;
        private final Deserializer<Object> deserializer;
        private final Serializer<Object> wrapSerializer;
        private final Deserializer<Object> wrapDeserializer;

        public AvroSerde(SerdeType type) {
            serializer = switch (type) {
                case APICURIO -> new AvroKafkaSerializer<>();
                case CONFLUENT -> new KafkaAvroSerializer();
            };
            deserializer = switch (type) {
                case APICURIO -> new AvroKafkaDeserializer<>();
                case CONFLUENT -> new KafkaAvroDeserializer();
            };

            var wrappedSerde = new WrappedSerde(serializer,deserializer,nativeMapper);
            wrapSerializer = wrappedSerde;
            wrapDeserializer = wrappedSerde;
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            serializer.configure(configs, isKey);
            deserializer.configure(configs, isKey);
        }

        @Override
        public Serializer<Object> serializer() {
            return wrapSerializer;
        }

        @Override
        public Deserializer<Object> deserializer() {
            return wrapDeserializer;
        }
    }

    private record WrappedSerde(Serializer<Object> serializer, Deserializer<Object> deserializer, NativeDataObjectMapper nativeMapper) implements Serializer<Object>, Deserializer<Object> {
        @Override
        public Object deserialize(final String topic, final byte[] data) {
            try {
                return mapper.toDataObject(deserializer.deserialize(topic, data));
            } catch (Exception e) {
                throw new ExecutionException("Error deserializing AVRO message from topic " + topic, e);
            }
        }

        @Override
        public Object deserialize(final String topic, final Headers headers, final byte[] data) {
            try {
                return mapper.toDataObject(deserializer.deserialize(topic, headers, data));
            } catch (Exception e) {
                throw new ExecutionException("Error deserializing AVRO message from topic " + topic, e);
            }
        }

        @Override
        public Object deserialize(final String topic, final Headers headers, final ByteBuffer data) {
            try {
                return mapper.toDataObject(deserializer.deserialize(topic, headers, data));
            } catch (Exception e) {
                throw new ExecutionException("Error deserializing AVRO message from topic " + topic, e);
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
                return serializer.serialize(topic, mapper.fromDataObject(nativeMapper.toDataObject(data)));
            } catch (Exception e) {
                throw new ExecutionException("Error serializing AVRO message to topic " + topic, e);
            }
        }

        @Override
        public byte[] serialize(final String topic, final Headers headers, final Object data) {
            try {
                return serializer.serialize(topic, headers, mapper.fromDataObject(nativeMapper.toDataObject(data)));
            } catch (Exception e) {
                throw new ExecutionException("Error serializing AVRO message to topic " + topic, e);
            }
        }

        @Override
        public void close() {
           serializer.close();
           deserializer.close();
        }
    }
}
