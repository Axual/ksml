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

import io.apicurio.registry.rest.client.RegistryClient;
import io.apicurio.registry.serde.jsonschema.JsonSchemaKafkaDeserializer;
import io.apicurio.registry.serde.jsonschema.JsonSchemaKafkaSerializer;
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
 * {@link RegistryClient} (useful for testing) and defaults to constructing
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
    private final RegistryClient registryClient;

    public ApicurioJsonSchemaSerdeSupplier() {
        this(null);
    }

    public ApicurioJsonSchemaSerdeSupplier(RegistryClient registryClient) {
        this.registryClient = registryClient;
    }

    @Override
    public String vendorName() {
        return "apicurio";
    }

    @Override
    public Serde<Object> get(DataType type, boolean isKey) {
        // Create a serde that injects Apicurio defaults while honoring user-supplied configs
        return new ApicurioJsonSchemaSerde(registryClient);
    }

    /**
     * Serde wrapper that injects default Apicurio configuration where not provided by the user.
     */
    static class ApicurioJsonSchemaSerde extends ConfigInjectionSerde {
        public ApicurioJsonSchemaSerde(RegistryClient registryClient) {
            this(Serdes.serdeFrom(
                    registryClient != null ? new JsonSchemaKafkaSerializer<>(registryClient) : new JsonSchemaKafkaSerializer<>(),
                    registryClient != null ? new JsonSchemaKafkaDeserializer<>(registryClient) : new JsonSchemaKafkaDeserializer<>()));
        }

        public ApicurioJsonSchemaSerde(Serde<Object> delegate) {
            super(delegate);
        }

        @Override
        protected Map<String, Object> modifyConfigs(Map<String, Object> configs, boolean isKey) {
            if (configs.getOrDefault("apicurio.registry.headers.enabled", false) == Boolean.FALSE) {
                // Enable payload encoding in a Confluent compatible way
                configs.putIfAbsent("apicurio.registry.artifact-resolver-strategy", "io.apicurio.registry.serde.strategy.TopicIdStrategy");
                configs.putIfAbsent("apicurio.registry.headers.enabled", false);
                configs.putIfAbsent("apicurio.registry.as-confluent", true);
                configs.putIfAbsent("apicurio.registry.use-id", "contentId");
                configs.putIfAbsent("apicurio.registry.id-handler", "io.apicurio.registry.serde.Legacy4ByteIdHandler");
                configs.putIfAbsent("apicurio.registry.serdes.json-schema.validation-enabled", true);
            }
            return configs;
        }
    }
}
