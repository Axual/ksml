package io.axual.ksml.notation;

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

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

import io.axual.ksml.data.mapper.NativeUserObjectMapper;
import io.axual.ksml.data.type.base.DataType;
import io.axual.ksml.data.type.base.ListType;
import io.axual.ksml.data.type.base.MapType;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.serde.JsonDeserializer;
import io.axual.ksml.serde.JsonSerializer;
import io.axual.ksml.util.DataUtil;

public class JsonNotation implements Notation {
    public static final String NAME = "JSON";
    private static final NativeUserObjectMapper mapper = new NativeUserObjectMapper();
    private final Map<String, Object> configs = new HashMap<>();

    public JsonNotation(Map<String, Object> configs) {
        this.configs.putAll(configs);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Serde<Object> getSerde(DataType type, boolean isKey) {
        if (type instanceof MapType || type instanceof ListType) {
            var result = new JsonSerde();
            result.configure(configs, isKey);
            return result;
        }
        throw new KSMLExecutionException("Json serde not found for data type " + type);
    }

    private static class JsonSerde implements Serde<Object> {
        private final JsonSerializer serializer = new JsonSerializer();
        private final JsonDeserializer deserializer = new JsonDeserializer();

        private final Serializer<Object> wrapSerializer = new Serializer<>() {
            @Override
            public byte[] serialize(String topic, Object data) {
                return serializer.serialize(topic, mapper.fromUserObject(DataUtil.asUserObject(data)));
            }
        };

        private final Deserializer<Object> wrapDeserializer = new Deserializer<>() {
            @Override
            public Object deserialize(String topic, byte[] data) {
                Object object = deserializer.deserialize(topic, data);
                return mapper.toUserObject(JsonNotation.NAME, object);
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
