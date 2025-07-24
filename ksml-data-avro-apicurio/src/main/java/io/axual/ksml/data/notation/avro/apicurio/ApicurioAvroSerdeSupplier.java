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
import io.apicurio.registry.serde.avro.AvroKafkaDeserializer;
import io.apicurio.registry.serde.avro.AvroKafkaSerializer;
import io.axual.ksml.data.notation.avro.AvroSerdeSupplier;
import io.axual.ksml.data.type.DataType;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class ApicurioAvroSerdeSupplier implements AvroSerdeSupplier {
    // Registry Client is mocked by tests
    private final RegistryClient registryClient;

    public ApicurioAvroSerdeSupplier() {
        this(null);
    }

    public ApicurioAvroSerdeSupplier(RegistryClient registryClient) {
        this.registryClient = registryClient;
    }

    @Override
    public String vendorName() {
        return "apicurio";
    }

    @Override
    public Serde<Object> get(DataType type, boolean isKey) {
        return new Serde<>() {
            private final Serializer<Object> serializer = registryClient != null ? new AvroKafkaSerializer<>(registryClient) : new AvroKafkaSerializer<>();
            private final Deserializer<Object> deserializer = registryClient != null ? new AvroKafkaDeserializer<>(registryClient) : new AvroKafkaDeserializer<>();

            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                serializer.configure(configs, isKey);
                deserializer.configure(configs, isKey);
            }

            @Override
            public Serializer<Object> serializer() {
                return serializer;
            }

            @Override
            public Deserializer<Object> deserializer() {
                return deserializer;
            }
        };
    }
}
