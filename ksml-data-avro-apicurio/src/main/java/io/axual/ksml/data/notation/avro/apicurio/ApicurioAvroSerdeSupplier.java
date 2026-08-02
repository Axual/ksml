package io.axual.ksml.data.notation.avro.apicurio;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - AVRO Apicurio
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

import io.apicurio.registry.resolver.client.RegistryClientFacade;
import io.apicurio.registry.resolver.config.SchemaResolverConfig;
import io.apicurio.registry.serde.Default4ByteIdHandler;
import io.apicurio.registry.serde.avro.AvroKafkaDeserializer;
import io.apicurio.registry.serde.avro.AvroKafkaSerializer;
import io.apicurio.registry.serde.config.SerdeConfig;
import io.apicurio.registry.serde.kafka.config.KafkaSerdeConfig;
import io.apicurio.registry.serde.strategy.TopicIdStrategy;
import io.axual.ksml.data.notation.avro.AvroSerdeSupplier;
import io.axual.ksml.data.serde.ConfigInjectionSerde;
import io.axual.ksml.data.type.DataType;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Map;

public class ApicurioAvroSerdeSupplier implements AvroSerdeSupplier {
    // Registry Client is mocked by tests
    private final RegistryClientFacade registryClient;

    public ApicurioAvroSerdeSupplier(RegistryClientFacade registryClient) {
        this.registryClient = registryClient;
    }

    @Override
    public Serde<Object> get(DataType type, boolean isKey) {
        return new ApicurioAvroSerde(registryClient);
    }

    /**
     * Serde that pins the Apicurio settings KSML relies on instead of taking the Apicurio v3 defaults, so
     * the on-wire format stays what KSML 1.x wrote. Every value uses {@code putIfAbsent}, so user
     * configuration always wins.
     */
    static class ApicurioAvroSerde extends ConfigInjectionSerde {
        ApicurioAvroSerde(RegistryClientFacade registryClient) {
            super(Serdes.serdeFrom(
                    registryClient != null ? new AvroKafkaSerializer<>(registryClient) : new AvroKafkaSerializer<>(),
                    registryClient != null ? new AvroKafkaDeserializer<>(registryClient) : new AvroKafkaDeserializer<>()));
        }

        // Delegate constructor, used by tests to verify the injected defaults without a real Apicurio serde.
        ApicurioAvroSerde(Serde<Object> delegate) {
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
            // Resolve pre-registered artifacts by coordinates instead of by content (issue #290).
            configs.putIfAbsent(SchemaResolverConfig.FIND_LATEST_ARTIFACT, true);
            return configs;
        }
    }
}
