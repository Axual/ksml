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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

import io.axual.ksml.data.mapper.NativeDataMapper;
import io.axual.ksml.data.object.UserObject;
import io.axual.ksml.data.type.base.DataType;
import io.axual.ksml.data.type.base.SimpleType;
import io.axual.ksml.util.DataUtil;

public class BinaryNotation implements Notation {
    public static final String NAME = "BINARY";
    private static final NativeDataMapper mapper = new NativeDataMapper();
    private final Map<String, Object> configs = new HashMap<>();
    private final Notation jsonNotation;

    public BinaryNotation(Map<String, Object> configs, Notation jsonNotation) {
        this.configs.putAll(configs);
        this.jsonNotation = jsonNotation;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Serde<Object> getSerde(DataType type, boolean isKey) {
        if (type instanceof SimpleType) {
            var result = new BinarySerde((Serde<Object>) Serdes.serdeFrom(type.containerClass()));
            result.configure(configs, isKey);
            return result;
        }
        // If not a simple type, then rely on JSON encoding
        return jsonNotation.getSerde(type, isKey);
    }

    private static class BinarySerde implements Serde<Object> {
        private final Serializer<Object> serializer;
        private final Deserializer<Object> deserializer;

        public BinarySerde(Serde<Object> serde) {
            this.serializer = serde.serializer();
            this.deserializer = serde.deserializer();
        }

        private final Serializer<Object> wrapSerializer = new Serializer<>() {
            @Override
            public byte[] serialize(String topic, Object data) {
                return serializer.serialize(topic, mapper.fromDataObject(DataUtil.asUserObject(data)));
            }
        };

        private final Deserializer<Object> wrapDeserializer = new Deserializer<>() {
            @Override
            public UserObject deserialize(String topic, byte[] data) {
                Object object = deserializer.deserialize(topic, data);
                return mapper.toDataObject(BinaryNotation.NAME, object);
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
