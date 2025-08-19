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

import io.axual.ksml.data.notation.Notation;
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.avro.AvroNotation;
import io.axual.ksml.data.notation.vendor.VendorNotation;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class ConfluentAvroNotationProviderTest {

    @Test
    @DisplayName("Provider exposes notation/vendor names for Confluent Avro")
    void providerMetadata_isCorrect() {
        var provider = new ConfluentAvroNotationProvider();
        assertThat(provider.notationName()).isEqualTo(AvroNotation.NOTATION_NAME);
        assertThat(provider.vendorName()).isEqualTo("confluent");
    }

    @Test
    @DisplayName("createNotation wires AvroNotation with Confluent serde supplier (default ctor, no SR client)")
    void createNotation_defaultConstructor_wiresConfluentSerdeSupplier() {
        // Given a provider without an explicit Schema Registry client
        var provider = new ConfluentAvroNotationProvider();
        var context = new NotationContext(AvroNotation.NOTATION_NAME, "confluent");

        // When
        Notation notation = provider.createNotation(context);

        // Then
        assertThat(notation).isInstanceOf(AvroNotation.class);
        assertThat(notation.name()).isEqualTo("confluent_" + AvroNotation.NOTATION_NAME);
        assertThat(notation.filenameExtension()).isEqualTo(".avsc");
        assertThat(notation.defaultType()).isEqualTo(AvroNotation.DEFAULT_TYPE);

        var vendorNotation = (VendorNotation) notation;
        var supplier = vendorNotation.serdeSupplier();
        assertThat((Object) supplier).isInstanceOf(ConfluentAvroSerdeSupplier.class);
        var confluentSupplier = (ConfluentAvroSerdeSupplier) supplier;
        assertThat(confluentSupplier.registryClient()).isNull();
    }

    @Test
    @DisplayName("createNotation wires supplier with provided SchemaRegistryClient")
    void createNotation_withRegistryClient_propagatesClient() {
        // Given
        SchemaRegistryClient registryClient = mock(SchemaRegistryClient.class);
        var provider = new ConfluentAvroNotationProvider(registryClient);
        var context = new NotationContext(AvroNotation.NOTATION_NAME, "confluent");

        // When
        Notation notation = provider.createNotation(context);

        // Then
        var supplier = ((VendorNotation) notation).serdeSupplier();
        assertThat((Object) supplier).isInstanceOf(ConfluentAvroSerdeSupplier.class);
        assertThat(((ConfluentAvroSerdeSupplier) supplier).registryClient()).isSameAs(registryClient);
    }
}
