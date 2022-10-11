package io.axual.ksml.avro;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 Axual B.V.
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

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.MapType;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.notation.Notation;
import io.axual.ksml.schema.DataSchema;
import io.axual.ksml.util.DataUtil;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class AvroNotation implements Notation {
    public static final String NOTATION_NAME = "AVRO";
    private static final AvroDataMapper mapper = new AvroDataMapper();
    private final Map<String, Object> configs = new HashMap<>();

    public AvroNotation(Map<String, Object> configs) {
        this.configs.putAll(configs);
    }

    @Override
    public String name() {
        return NOTATION_NAME;
    }

    @Override
    public Serde<Object> getSerde(DataType type, DataSchema schema, boolean isKey) {
        if (type instanceof MapType) {
            var result = new AvroSerde();
            result.configure(configs, isKey);
            return result;
        }
        throw new KSMLExecutionException("Avro serde not found for data type " + type);
    }

    private static class AvroSerde implements Serde<Object> {
        private final Serializer<Object> serializer = new KafkaAvroSerializer();
        private final Deserializer<Object> deserializer = new KafkaAvroDeserializer();

        private final Serializer<Object> wrapSerializer = new Serializer<>() {
            @Override
            public byte[] serialize(String topic, Object data) {
                return serializer.serialize(topic, mapper.fromDataObject(DataUtil.asUserObject(data)));
            }
        };

        private final Deserializer<Object> wrapDeserializer = new Deserializer<>() {
            @Override
            public Object deserialize(String topic, byte[] data) {
                GenericRecord object = (GenericRecord) deserializer.deserialize(topic, data);
                return mapper.toDataObject(object);
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
