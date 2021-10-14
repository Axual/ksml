package io.axual.ksml;

/*-
 * ========================LICENSE_START=================================
 * KSML for Axual
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

import io.axual.ksml.avro.AvroDataMapper;
import io.axual.ksml.data.mapper.DataMapper;
import io.axual.ksml.data.type.user.UserRecordType;
import io.axual.ksml.data.type.user.UserType;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.notation.AvroNotation;
import io.axual.ksml.notation.Notation;
import io.axual.ksml.serde.UnknownTypeSerde;
import io.axual.ksml.util.DataUtil;
import io.axual.streams.proxy.axual.AxualSerdeConfig;

public class AxualAvroNotation implements Notation {
    private static final DataMapper<Object> mapper = new AvroDataMapper();
    private final Map<String, Object> configs = new HashMap<>();

    public AxualAvroNotation(Map<String, Object> configs) {
        this.configs.putAll(configs);
        this.configs.put(AxualSerdeConfig.BACKING_KEY_SERDE_CONFIG, UnknownTypeSerde.class.getName());
        this.configs.put(AxualSerdeConfig.BACKING_VALUE_SERDE_CONFIG, UnknownTypeSerde.class.getName());
    }

    @Override
    public String name() {
        return AvroNotation.NAME;
    }

    public Serde<Object> getSerde(UserType type, boolean isKey) {
        if (type instanceof UserRecordType) {
            var result = new AvroSerde(configs, (UserRecordType) type, isKey);
            result.configure(configs, isKey);
            return result;
        }
        throw new KSMLExecutionException("Serde not found for data type " + type);
    }

    private class AvroSerde implements Serde<Object> {
        private final Serializer<GenericRecord> serializer;
        private final Deserializer<GenericRecord> deserializer;

        public AvroSerde(Map<String, Object> configs, UserRecordType type, boolean isKey) {
            AxualAvroSerde serde = new AxualAvroSerde(configs, type, isKey);
            serializer = serde.serializer();
            deserializer = serde.deserializer();
        }

        private final Serializer<Object> wrapSerializer = new Serializer<>() {
            @Override
            public byte[] serialize(String topic, Object data) {
                var object = mapper.fromDataObject(DataUtil.asUserObject(data));
                if (object instanceof GenericRecord) {
                    return serializer.serialize(topic, (GenericRecord) object);
                }
                throw new KSMLExecutionException("Can not serialize using Avro: " + object.getClass().getSimpleName());
            }
        };

        private final Deserializer<Object> wrapDeserializer = new Deserializer<>() {
            @Override
            public Object deserialize(String topic, byte[] data) {
                GenericRecord object = deserializer.deserialize(topic, data);
                return mapper.toDataObject(AvroNotation.NAME, object);
            }
        };

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
