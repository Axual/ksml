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

import io.apicurio.registry.resolver.client.RegistryClientFacade;
import io.apicurio.registry.serde.protobuf.ProtobufKafkaDeserializer;
import io.apicurio.registry.serde.protobuf.ProtobufKafkaSerializer;
import io.axual.ksml.data.serde.SerdeSupplier;
import io.axual.ksml.data.type.DataType;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class ApicurioProtobufSerdeSupplier implements SerdeSupplier {
    private final RegistryClientFacade registryClient;

    public ApicurioProtobufSerdeSupplier() {
        this(null);
    }

    public ApicurioProtobufSerdeSupplier(RegistryClientFacade registryClient) {
        this.registryClient = registryClient;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Serde<Object> get(DataType type, boolean isKey) {
        return (Serde) Serdes.serdeFrom(
                registryClient != null ? new ProtobufKafkaSerializer<>(registryClient) : new ProtobufKafkaSerializer<>(),
                registryClient != null ? new ProtobufKafkaDeserializer<>(registryClient) : new ProtobufKafkaDeserializer<>());
    }
}
