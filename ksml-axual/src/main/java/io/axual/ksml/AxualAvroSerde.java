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

import java.util.Map;

import io.axual.client.proxy.generic.registry.ProxyChain;
import io.axual.client.proxy.resolving.generic.ResolvingProxyConfig;
import io.axual.common.resolver.TopicPatternResolver;
import io.axual.ksml.data.type.RecordType;
import io.axual.serde.avro.BaseAvroDeserializer;
import io.axual.serde.avro.GenericAvroDeserializer;
import io.axual.serde.avro.GenericAvroSerializer;
import io.axual.streams.config.StreamRunnerConfig;
import io.axual.streams.proxy.axual.AxualSerde;
import io.axual.streams.proxy.axual.AxualSerdeConfig;

public class AxualAvroSerde extends AxualSerde<GenericRecord> {
    private final RecordType type;
    private static final Serde<GenericRecord> generatingSerde = new Serde<>() {
        @Override
        public Serializer<GenericRecord> serializer() {
            return new GenericAvroSerializer<>();
        }

        @Override
        public Deserializer<GenericRecord> deserializer() {
            return new GenericAvroDeserializer<>();
        }
    };

    public AxualAvroSerde(Map<String, Object> configs, RecordType type, boolean isKey) {
        super(getConfig(configs, type), isKey);
        this.type = type;
    }

    private static Map<String, Object> getConfig(Map<String, Object> configs, RecordType type) {
        final ProxyChain chain = StreamRunnerConfig.DEFAULT_PROXY_CHAIN;
        configs.put(AxualSerdeConfig.KEY_SERDE_CHAIN_CONFIG, chain);
        configs.put(AxualSerdeConfig.VALUE_SERDE_CHAIN_CONFIG, chain);
        configs.put(AxualSerdeConfig.BACKING_KEY_SERDE_CONFIG, generatingSerde);
        configs.put(AxualSerdeConfig.BACKING_VALUE_SERDE_CONFIG, generatingSerde);
        configs.put(ResolvingProxyConfig.TOPIC_RESOLVER_CONFIG, TopicPatternResolver.class.getName());
        configs.put(TopicPatternResolver.TOPIC_PATTERN_CONFIG, "{tenant}-{instance}-{environment}-{topic}");
        configs.put(BaseAvroDeserializer.SPECIFIC_KEY_SCHEMA_CONFIG, type.schema());
        configs.put(BaseAvroDeserializer.SPECIFIC_VALUE_SCHEMA_CONFIG, type.schema());
        return configs;
    }

    public RecordType getType() {
        return type;
    }
}
