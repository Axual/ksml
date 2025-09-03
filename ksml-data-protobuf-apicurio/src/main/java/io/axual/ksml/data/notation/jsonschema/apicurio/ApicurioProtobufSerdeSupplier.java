package io.axual.ksml.data.notation.jsonschema.apicurio;

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

import io.apicurio.registry.rest.client.RegistryClient;
import io.apicurio.registry.serde.SerdeConfig;
import io.apicurio.registry.serde.SerdeHeaders;
import io.apicurio.registry.serde.protobuf.ProtobufKafkaDeserializer;
import io.apicurio.registry.serde.protobuf.ProtobufKafkaSerializer;
import io.axual.ksml.data.notation.protobuf.ProtobufSerdeSupplier;
import io.axual.ksml.data.serde.ConfigInjectionSerde;
import io.axual.ksml.data.serde.HeaderFilterSerde;
import io.axual.ksml.data.type.DataType;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Map;
import java.util.Set;

public class ApicurioProtobufSerdeSupplier implements ProtobufSerdeSupplier {
    private final RegistryClient registryClient;

    public ApicurioProtobufSerdeSupplier() {
        this(null);
    }

    public ApicurioProtobufSerdeSupplier(RegistryClient registryClient) {
        this.registryClient = registryClient;
    }

    @Override
    public String vendorName() {
        return "apicurio";
    }

    @Override
    public Serde<Object> get(DataType type, boolean isKey) {
        return new ApicurioProtobufSerde(registryClient);
    }

    static class ApicurioProtobufSerde extends ConfigInjectionSerde {
        private final HeaderFilterSerde delegate;

        @SuppressWarnings("unchecked")
        public ApicurioProtobufSerde(RegistryClient registryClient) {
            this(new HeaderFilterSerde((Serde) Serdes.serdeFrom(
                    registryClient != null ? new ProtobufKafkaSerializer<>(registryClient) : new ProtobufKafkaSerializer<>(),
                    registryClient != null ? new ProtobufKafkaDeserializer<>(registryClient) : new ProtobufKafkaDeserializer<>())));
        }

        public ApicurioProtobufSerde(HeaderFilterSerde delegate) {
            super(delegate);
            this.delegate = delegate;
        }

        @Override
        public Map<String, Object> modifyConfigs(Map<String, Object> configs, boolean isKey) {
            // Configure header filtering
            final String messageTypeHeaderName;
            if (isKey) {
                messageTypeHeaderName = configs.getOrDefault(SerdeConfig.HEADER_KEY_MESSAGE_TYPE_OVERRIDE_NAME, SerdeHeaders.HEADER_KEY_MESSAGE_TYPE).toString();
            } else {
                messageTypeHeaderName = configs.getOrDefault(SerdeConfig.HEADER_VALUE_MESSAGE_TYPE_OVERRIDE_NAME, SerdeHeaders.HEADER_VALUE_MESSAGE_TYPE).toString();
            }
            delegate.filteredHeaders(Set.of(messageTypeHeaderName));

            // Enable payload encoding by default
            configs.putIfAbsent("apicurio.registry.artifact-resolver-strategy", "io.apicurio.registry.serde.strategy.TopicIdStrategy");
            configs.putIfAbsent("apicurio.registry.headers.enabled", false);
            configs.putIfAbsent("apicurio.registry.as-confluent", true);
            configs.putIfAbsent("apicurio.registry.use-id", "contentId");
            return configs;
        }
    }
}
