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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

import io.axual.ksml.data.mapper.JsonDataObjectMapper;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.MapType;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.schema.DataSchema;
import io.axual.ksml.util.DataUtil;

public class JsonNotation implements Notation {
    public static final String NOTATION_NAME = "JSON";
    private static final JsonDataObjectMapper jsonMapper = new JsonDataObjectMapper();
    private final Map<String, Object> configs = new HashMap<>();

    public JsonNotation(Map<String, Object> configs) {
        this.configs.putAll(configs);
    }

    @Override
    public String name() {
        return NOTATION_NAME;
    }

    @Override
    public Serde<Object> getSerde(DataType type, DataSchema schema, boolean isKey) {
        if (type instanceof MapType || type instanceof ListType) {
            var result = new JsonSerde();
            result.configure(configs, isKey);
            return result;
        }
        throw new KSMLExecutionException("Json serde not found for data type " + type);
    }

    private static class JsonSerde implements Serde<Object> {
        private final StringSerializer serializer = new StringSerializer();
        private final StringDeserializer deserializer = new StringDeserializer();

        private final Serializer<Object> wrapSerializer = (topic, data) -> {
            var json = jsonMapper.fromDataObject(DataUtil.asDataObject(data));
            return serializer.serialize(topic, json);
        };

        private final Deserializer<Object> wrapDeserializer = (topic, data) -> {
            String json = deserializer.deserialize(topic, data);
            return jsonMapper.toDataObject(json);
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
