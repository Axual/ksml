package io.axual.ksml.data.notation.protobuf.confluent;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - Protobuf Confluent
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

import io.axual.ksml.data.notation.protobuf.ProtobufSerdeSupplier;
import io.axual.ksml.data.type.DataType;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import lombok.Getter;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class ConfluentProtobufSerdeSupplier implements ProtobufSerdeSupplier {
    // Registry Client is mocked by tests
    @Getter
    private final SchemaRegistryClient registryClient;

    public ConfluentProtobufSerdeSupplier() {
        this(null);
    }

    public ConfluentProtobufSerdeSupplier(SchemaRegistryClient registryClient) {
        this.registryClient = registryClient;
    }

    @Override
    public String vendorName() {
        return "confluent";
    }

    @Override
    public Serde<Object> get(DataType type, boolean isKey) {
        return (Serde) Serdes.serdeFrom(
                registryClient != null ? new KafkaProtobufSerializer<>(registryClient) : new KafkaProtobufSerializer<>(),
                registryClient != null ? new KafkaProtobufDeserializer<>(registryClient) : new KafkaProtobufDeserializer<>());
    }
}
