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

import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.type.DataType;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class ConfluentAvroSerdeSupplierTest {

    @Test
    @DisplayName("vendorName returns 'confluent'")
    void vendorName_isConfluent() {
        var supplier = new ConfluentAvroSerdeSupplier(new NotationContext("avro", "confluent"));
        assertThat(supplier.vendorName()).isEqualTo("confluent");
    }

    @Test
    @DisplayName("get() returns WrapperSerde with KafkaAvroSerializer/Deserializer (no SR client)")
    void get_returnsSerdeWithConfluentSerdes_withoutClient() {
        // Given
        var serdeSupplier = new ConfluentAvroSerdeSupplier(new NotationContext("avro", "confluent"));

        // When
        var serde = serdeSupplier.get(DataType.UNKNOWN, false);

        // Then
        assertThat(serde).isInstanceOf(Serdes.WrapperSerde.class);
        assertThat(serde.serializer()).isInstanceOf(KafkaAvroSerializer.class);
        assertThat(serde.deserializer()).isInstanceOf(KafkaAvroDeserializer.class);
        assertThat(serdeSupplier.registryClient()).isNull();
    }

    @Test
    @DisplayName("get() returns WrapperSerde with KafkaAvroSerializer/Deserializer (with SR client)")
    void get_returnsSerdeWithConfluentSerdes_withClient() {
        // Given
        var client = mock(SchemaRegistryClient.class);
        var supplier = new ConfluentAvroSerdeSupplier(new NotationContext("avro", "confluent"), client);

        // When
        var serde = supplier.get(DataType.UNKNOWN, true);

        // Then
        assertThat(serde).isInstanceOf(Serdes.WrapperSerde.class);
        assertThat(serde.serializer()).isInstanceOf(KafkaAvroSerializer.class);
        assertThat(serde.deserializer()).isInstanceOf(KafkaAvroDeserializer.class);
        // We can't inspect the client inside Confluent classes without reflection; instead, verify
        // the supplier exposes the provided client instance.
        assertThat(supplier.registryClient()).isSameAs(client);
    }
}
