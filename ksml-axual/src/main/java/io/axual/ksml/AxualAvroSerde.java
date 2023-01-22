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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

import io.axual.client.proxy.resolving.generic.ResolvingProxyConfig;
import io.axual.common.resolver.TopicPatternResolver;
import io.axual.ksml.notation.avroschema.AvroSchemaMapper;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.serde.avro.BaseAvroDeserializer;
import io.axual.streams.config.StreamRunnerConfig;
import io.axual.streams.proxy.axual.AxualSerde;
import io.axual.streams.proxy.axual.AxualSerdeConfig;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class AxualAvroSerde extends AxualSerde<GenericRecord> {
    private static final AvroSchemaMapper schemaMapper = new AvroSchemaMapper();
    private static final Serde<Object> generatingSerde = new Serde<>() {
        @Override
        public Serializer<Object> serializer() {
            return new KafkaAvroSerializer();
        }

        @Override
        public Deserializer<Object> deserializer() {
            return new KafkaAvroDeserializer();
        }
    };

    public AxualAvroSerde(Map<String, Object> configs, DataSchema schema, boolean isKey) {
        super(getConfig(configs, schema), isKey);
    }

    private static Map<String, Object> getConfig(Map<String, Object> configs, DataSchema schema) {
        final var avroSchema = schemaMapper.fromDataSchema(schema);
        final var chain = StreamRunnerConfig.DEFAULT_PROXY_CHAIN;
        configs.put(AxualSerdeConfig.KEY_SERDE_CHAIN_CONFIG, chain);
        configs.put(AxualSerdeConfig.VALUE_SERDE_CHAIN_CONFIG, chain);
        configs.put(AxualSerdeConfig.BACKING_KEY_SERDE_CONFIG, generatingSerde);
        configs.put(AxualSerdeConfig.BACKING_VALUE_SERDE_CONFIG, generatingSerde);
        configs.put(ResolvingProxyConfig.TOPIC_RESOLVER_CONFIG, TopicPatternResolver.class.getName());
        configs.put(TopicPatternResolver.TOPIC_PATTERN_CONFIG, "{tenant}-{instance}-{environment}-{topic}");
        configs.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
        if (avroSchema != null && avroSchema.getType() == Schema.Type.RECORD) {
            configs.put(BaseAvroDeserializer.SPECIFIC_KEY_SCHEMA_CONFIG, avroSchema);
            configs.put(BaseAvroDeserializer.SPECIFIC_VALUE_SCHEMA_CONFIG, avroSchema);
        }

        // Copy the SSL configuration so the schema registry also gets it as its config
        Map<String, Object> copy = new HashMap<>(configs);
        copy.keySet().stream().filter(k -> k.startsWith("ssl.")).forEach(k -> {
            final Object value = copy.get(k);
            // Explode passwords into their string literals
            if (value instanceof Password password) {
                configs.put("schema.registry." + k, password.value());
            } else {
                configs.put("schema.registry." + k, value);
            }
        });
        return configs;
    }
}
