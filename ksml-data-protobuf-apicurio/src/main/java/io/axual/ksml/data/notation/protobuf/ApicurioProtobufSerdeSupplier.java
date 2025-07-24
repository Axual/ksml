package io.axual.ksml.data.notation.protobuf;

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
import io.axual.ksml.data.serde.HeaderFilterSerde;
import io.axual.ksml.data.serde.WrappedSerde;
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
        final var serde = new HeaderFilterSerde((Serde) Serdes.serdeFrom(
                registryClient != null ? new ProtobufKafkaSerializer<>(registryClient) : new ProtobufKafkaSerializer<>(),
                registryClient != null ? new ProtobufKafkaDeserializer<>(registryClient) : new ProtobufKafkaDeserializer<>()));
        return new WrappedSerde(serde) {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                final String messageTypeHeaderName;
                if (isKey) {
                    messageTypeHeaderName = (String) ((Map<String, Object>) configs).getOrDefault(SerdeConfig.HEADER_KEY_MESSAGE_TYPE_OVERRIDE_NAME, SerdeHeaders.HEADER_KEY_MESSAGE_TYPE);
                } else {
                    messageTypeHeaderName = (String) ((Map<String, Object>) configs).getOrDefault(SerdeConfig.HEADER_VALUE_MESSAGE_TYPE_OVERRIDE_NAME, SerdeHeaders.HEADER_VALUE_MESSAGE_TYPE);
                }
                serde.filteredHeaders(Set.of(messageTypeHeaderName));
                super.configure(configs, isKey);
            }
        };
    }
}
