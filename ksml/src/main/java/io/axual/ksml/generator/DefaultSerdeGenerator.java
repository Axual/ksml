package io.axual.ksml.generator;

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
import java.util.Properties;

import io.axual.ksml.exception.KSMLTopologyException;
import io.axual.ksml.type.AvroType;
import io.axual.ksml.type.DataType;
import io.axual.ksml.type.SimpleType;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class DefaultSerdeGenerator implements SerdeGenerator {
    private final Map<String, Object> configs;

    public DefaultSerdeGenerator(Map<String, Object> configs) {
        this.configs = configs;
    }

    public DefaultSerdeGenerator(Properties properties) {
        configs = new HashMap<>();
        properties.forEach((k, v) -> configs.put((String) k, v));
    }

    public Serde<Object> getSerdeForType(final DataType type, boolean isKey) {
        if (type instanceof AvroType) {
            Serializer<Object> serializer = new KafkaAvroSerializer();
            serializer.configure(configs, isKey);
            Deserializer<Object> deserializer = new KafkaAvroDeserializer();
            deserializer.configure(configs, isKey);
            return new Serdes.WrapperSerde<>(serializer, deserializer);
        }
        if (type instanceof SimpleType) {
            return (Serde<Object>) Serdes.serdeFrom(((SimpleType) type).type);
        }
        throw new KSMLTopologyException("Serde not found");
    }
}
