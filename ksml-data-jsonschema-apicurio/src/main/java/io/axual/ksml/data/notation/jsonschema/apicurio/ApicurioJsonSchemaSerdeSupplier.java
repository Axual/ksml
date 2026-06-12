package io.axual.ksml.data.notation.jsonschema.apicurio;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - JSON Schema Apicurio
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
import io.apicurio.registry.serde.config.SerdeConfig;
import io.apicurio.registry.serde.jsonschema.JsonSchemaKafkaDeserializer;
import io.apicurio.registry.serde.jsonschema.JsonSchemaKafkaSerializer;
import io.apicurio.registry.serde.kafka.config.KafkaSerdeConfig;
import io.apicurio.registry.serde.strategy.TopicIdStrategy;
import io.axual.ksml.data.notation.jsonschema.JsonSchemaSerdeSupplier;
import io.axual.ksml.data.serde.ConfigInjectionSerde;
import io.axual.ksml.data.type.DataType;
import lombok.Getter;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Map;

/**
 * JsonSchema Serde supplier for the Apicurio Registry vendor implementation.
 *
 * <p>This class implements {@link JsonSchemaSerdeSupplier} and produces a Kafka {@link Serde}
 * backed by Apicurio's JsonSchema serializer/deserializer. It optionally accepts a
 * {@link RegistryClientFacade} (useful for testing) and defaults to constructing
 * serializer/deserializer instances without an explicit client when null.</p>
 *
 * <p>The returned Serde is wrapped in a {@link ConfigInjectionSerde} to apply sensible default
 * configuration keys for Apicurio (artifact resolver strategy, headers mode, confluent compatibility,
 * and id handling). User-provided configuration always takes precedence.</p>
 */
public class ApicurioJsonSchemaSerdeSupplier implements JsonSchemaSerdeSupplier {
    /**
     * Optional Apicurio registry client; primarily used by tests.
     */
    @Getter
    private final RegistryClientFacade registryClient;

    public ApicurioJsonSchemaSerdeSupplier() {
        this(null);
    }

    public ApicurioJsonSchemaSerdeSupplier(RegistryClientFacade registryClient) {
        this.registryClient = registryClient;
    }

    @Override
    public Serde<Object> get(DataType type, boolean isKey) {
        // Create a serde that injects Apicurio defaults while honoring user-supplied configs
        return new ApicurioJsonSchemaSerde(registryClient);
    }

    /**
     * Serde wrapper that injects the default Apicurio configuration where not provided by the user.
     */
    static class ApicurioJsonSchemaSerde extends ConfigInjectionSerde {
        public ApicurioJsonSchemaSerde(RegistryClientFacade registryClient) {
            this(Serdes.serdeFrom(
                    registryClient != null ? new JsonSchemaKafkaSerializer<>(registryClient) : new JsonSchemaKafkaSerializer<>(),
                    registryClient != null ? new JsonSchemaKafkaDeserializer<>(registryClient) : new JsonSchemaKafkaDeserializer<>()));
        }

        public ApicurioJsonSchemaSerde(Serde<Object> delegate) {
            super(delegate);
        }

        @Override
        protected Map<String, Object> modifyConfigs(Map<String, Object> configs, boolean isKey) {
            if (configs.getOrDefault(KafkaSerdeConfig.ENABLE_HEADERS, false) == Boolean.FALSE ||
                    configs.getOrDefault(KafkaSerdeConfig.ENABLE_HEADERS, "false").equals("false")) {
                // Enable payload encoding in a Confluent compatible way
                configs.putIfAbsent(SchemaResolverConfig.ARTIFACT_RESOLVER_STRATEGY, TopicIdStrategy.class.getCanonicalName());
                configs.putIfAbsent(KafkaSerdeConfig.ENABLE_HEADERS, false);
                configs.putIfAbsent(SerdeConfig.USE_ID, "contentId");
                configs.putIfAbsent(SerdeConfig.ID_HANDLER, Default4ByteIdHandler.class.getCanonicalName());
                configs.putIfAbsent("apicurio.registry.serdes.json-schema.validation-enabled", true);
            }
            return configs;
        }
    }
}
