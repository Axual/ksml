package io.axual.ksml.data.notation.jsonschema;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - JSON Schema Confluent
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
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

import io.axual.ksml.data.type.DataType;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import lombok.Getter;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class ConfluentJsonSchemaSerdeSupplier implements JsonSchemaSerdeSupplier {
    @Getter
    private final SchemaRegistryClient registryClient;

    public ConfluentJsonSchemaSerdeSupplier() {
        this(null);
    }

    public ConfluentJsonSchemaSerdeSupplier(SchemaRegistryClient registryClient) {
        this.registryClient = registryClient;
    }

    @Override
    public String vendorName() {
        return "confluent";
    }

    @Override
    public Serde<Object> get(DataType type, boolean isKey) {
        return new Serde<>() {
            @Getter
            private final Serializer<Object> serializer = registryClient != null ? new KafkaJsonSchemaSerializer<>(registryClient) : new KafkaJsonSchemaSerializer<>();
            @Getter
            private final Deserializer<Object> deserializer = registryClient != null ? new KafkaJsonSchemaDeserializer<>(registryClient) : new KafkaJsonSchemaDeserializer<>();

            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                serializer.configure(configs, isKey);
                deserializer.configure(configs, isKey);
            }
        };
    }
}
