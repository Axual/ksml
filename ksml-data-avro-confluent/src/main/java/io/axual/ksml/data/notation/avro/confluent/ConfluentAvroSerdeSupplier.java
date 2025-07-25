package io.axual.ksml.data.notation.avro.confluent;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - AVRO Confluent
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

import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.avro.AvroDataObjectMapper;
import io.axual.ksml.data.notation.avro.AvroSerdeSupplier;
import io.axual.ksml.data.type.DataType;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.Getter;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class ConfluentAvroSerdeSupplier implements AvroSerdeSupplier {
    private static final AvroDataObjectMapper MAPPER = new AvroDataObjectMapper();
    private final NotationContext notationContext;
    // Registry Client is mocked by tests
    @Getter
    private final SchemaRegistryClient registryClient;

    public ConfluentAvroSerdeSupplier(NotationContext notationContext) {
        this(notationContext, null);
    }

    public ConfluentAvroSerdeSupplier(NotationContext notationContext, SchemaRegistryClient registryClient) {
        this.notationContext = notationContext;
        this.registryClient = registryClient;
    }

    @Override
    public String vendorName() {
        return "confluent";
    }

    @Override
    public Serde<Object> get(DataType type, boolean isKey) {
        return Serdes.serdeFrom(
                registryClient != null ? new KafkaAvroSerializer(registryClient) : new KafkaAvroSerializer(),
                registryClient != null ? new KafkaAvroDeserializer(registryClient) : new KafkaAvroDeserializer());
    }
}
