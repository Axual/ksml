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

import io.apicurio.registry.rest.client.RegistryClient;
import io.apicurio.registry.serde.Legacy4ByteIdHandler;
import io.apicurio.registry.serde.SerdeConfig;
import io.apicurio.registry.serde.avro.AvroKafkaDeserializer;
import io.apicurio.registry.serde.avro.AvroKafkaSerializer;
import io.apicurio.registry.serde.strategy.TopicIdStrategy;
import io.axual.ksml.data.notation.avro.AvroSerdeSupplier;
import io.axual.ksml.data.serde.ConfigInjectionSerde;
import io.axual.ksml.data.type.DataType;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Map;

public class ApicurioAvroSerdeSupplier implements AvroSerdeSupplier {
    // Registry Client is mocked by tests
    private final RegistryClient registryClient;

    public ApicurioAvroSerdeSupplier(RegistryClient registryClient) {
        this.registryClient = registryClient;
    }

    @Override
    public String vendorName() {
        return "apicurio";
    }

    @Override
    public Serde<Object> get(DataType type, boolean isKey) {
        return new ApicurioAvroSerde(registryClient);
    }

    static class ApicurioAvroSerde extends ConfigInjectionSerde {
        public ApicurioAvroSerde(RegistryClient registryClient) {
            this(Serdes.serdeFrom(
                    registryClient != null ? new AvroKafkaSerializer<>(registryClient) : new AvroKafkaSerializer<>(),
                    registryClient != null ? new AvroKafkaDeserializer<>(registryClient) : new AvroKafkaDeserializer<>()));
        }

        public ApicurioAvroSerde(Serde<Object> delegate) {
            super(delegate);
        }

        @Override
        protected Map<String, Object> modifyConfigs(Map<String, Object> configs, boolean isKey) {
            if (configs.getOrDefault(SerdeConfig.ENABLE_HEADERS, false) == Boolean.FALSE ||
                    configs.getOrDefault(SerdeConfig.ENABLE_HEADERS, "false").equals("false")) {
                // Enable payload encoding in a Confluent compatible way
                configs.putIfAbsent(SerdeConfig.ARTIFACT_RESOLVER_STRATEGY, TopicIdStrategy.class.getCanonicalName());
                configs.putIfAbsent(SerdeConfig.ENABLE_HEADERS, false);
                configs.putIfAbsent(SerdeConfig.ENABLE_CONFLUENT_ID_HANDLER, true);
                configs.putIfAbsent(SerdeConfig.USE_ID, "contentId");
                configs.putIfAbsent(SerdeConfig.ID_HANDLER, Legacy4ByteIdHandler.class.getCanonicalName());
            }
            return configs;
        }
    }
}
