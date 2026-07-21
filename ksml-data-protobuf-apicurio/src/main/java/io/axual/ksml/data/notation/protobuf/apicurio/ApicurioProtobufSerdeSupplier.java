package io.axual.ksml.data.notation.protobuf.apicurio;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - Protobuf Apicurio
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

import io.apicurio.registry.resolver.config.SchemaResolverConfig;
import io.apicurio.registry.serde.Default4ByteIdHandler;
import io.apicurio.registry.serde.config.SerdeConfig;
import io.apicurio.registry.serde.kafka.config.KafkaSerdeConfig;
import io.apicurio.registry.resolver.client.RegistryClientFacade;
import io.apicurio.registry.serde.protobuf.ProtobufKafkaDeserializer;
import io.apicurio.registry.serde.protobuf.ProtobufKafkaSerializer;
import io.apicurio.registry.serde.strategy.TopicIdStrategy;
import io.axual.ksml.data.serde.ConfigInjectionSerde;
import io.axual.ksml.data.serde.SerdeSupplier;
import io.axual.ksml.data.type.DataType;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Map;

public class ApicurioProtobufSerdeSupplier implements SerdeSupplier {
    private final RegistryClientFacade registryClient;

    public ApicurioProtobufSerdeSupplier() {
        this(null);
    }

    public ApicurioProtobufSerdeSupplier(RegistryClientFacade registryClient) {
        this.registryClient = registryClient;
    }

    @Override
    public Serde<Object> get(DataType type, boolean isKey) {
        return new ApicurioProtobufSerde(registryClient);
    }

    /**
     * Serde that pins the Apicurio serde configuration KSML relies on, rather than depending on the
     * Apicurio v3 defaults. When headers are not enabled it forces the Confluent-compatible id format
     * (a 4-byte content id in the message payload, not in Kafka headers), matching KSML 1.x. Every value
     * uses {@code putIfAbsent}, so user-supplied configuration always wins.
     */
    static class ApicurioProtobufSerde extends ConfigInjectionSerde {
        @SuppressWarnings("unchecked")
        ApicurioProtobufSerde(RegistryClientFacade registryClient) {
            super((Serde<Object>) (Serde<?>) Serdes.serdeFrom(
                    registryClient != null ? new ProtobufKafkaSerializer<>(registryClient) : new ProtobufKafkaSerializer<>(),
                    registryClient != null ? new ProtobufKafkaDeserializer<>(registryClient) : new ProtobufKafkaDeserializer<>()));
        }

        // Delegate constructor, used by tests to verify the injected defaults without a real Apicurio serde.
        ApicurioProtobufSerde(Serde<Object> delegate) {
            super(delegate);
        }

        @Override
        protected Map<String, Object> modifyConfigs(Map<String, Object> configs, boolean isKey) {
            if (configs.getOrDefault(KafkaSerdeConfig.ENABLE_HEADERS, false) == Boolean.FALSE ||
                    configs.getOrDefault(KafkaSerdeConfig.ENABLE_HEADERS, "false").equals("false")) {
                // Encode the schema id in the payload in the Confluent-compatible way.
                configs.putIfAbsent(SchemaResolverConfig.ARTIFACT_RESOLVER_STRATEGY, TopicIdStrategy.class.getCanonicalName());
                configs.putIfAbsent(KafkaSerdeConfig.ENABLE_HEADERS, false);
                configs.putIfAbsent(SerdeConfig.USE_ID, "contentId");
                configs.putIfAbsent(SerdeConfig.ID_HANDLER, Default4ByteIdHandler.class.getCanonicalName());
            }
            return configs;
        }
    }
}
