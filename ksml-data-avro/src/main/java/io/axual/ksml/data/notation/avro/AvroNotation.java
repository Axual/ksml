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
import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.exception.ExecutionException;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.notation.Notation;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.MapType;
import io.axual.ksml.data.type.StructType;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class AvroNotation implements Notation {
    public static final String NOTATION_NAME = "AVRO";
    public static final DataType DEFAULT_TYPE = new StructType();
    private static final AvroDataObjectMapper mapper = new AvroDataObjectMapper();
    private final NativeDataObjectMapper nativeMapper;
    private final Map<String, ?> serdeConfigs;
    private Serde<Object> keySerde;
    private Serde<Object> valueSerde;

    public AvroNotation(NativeDataObjectMapper nativeMapper, Map<String, ?> configs) {
        this.nativeMapper = nativeMapper;
        this.serdeConfigs = ImmutableMap.copyOf(configs);
    }

    @Override
    public String name() {
        return NOTATION_NAME;
    }

    @Override
    public Serde<Object> serde(DataType type, boolean isKey) {
        if (!(type instanceof MapType)) throw new DataException("AVRO serde can not be applied to type " + type);

        // Create the serdes only upon request to prevent error messages on missing SR url configs if AVRO is not used
        if (keySerde == null) {
            keySerde = new AvroSerde();
            keySerde.configure(serdeConfigs, true);
        }
        if (valueSerde == null) {
            valueSerde = new AvroSerde();
            valueSerde.configure(serdeConfigs, false);
        }
        return isKey ? keySerde : valueSerde;
    }

    private class AvroSerde implements Serde<Object> {
        private final Serializer<Object> serializer = new KafkaAvroSerializer();
        private final Deserializer<Object> deserializer = new KafkaAvroDeserializer();

        private final Serializer<Object> wrapSerializer =
                (topic, data) -> {
                    try {
                        return serializer.serialize(topic, mapper.fromDataObject(nativeMapper.toDataObject(data)));
                    } catch (Exception e) {
                        throw new ExecutionException("Error serializing AVRO message to topic " + topic, e);
                    }
                };

        private final Deserializer<Object> wrapDeserializer =
                (topic, data) -> {
                    try {
                        return mapper.toDataObject(deserializer.deserialize(topic, data));
                    } catch (Exception e) {
                        throw new ExecutionException("Error deserializing AVRO message from topic " + topic, e);
                    }
                };

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
}
