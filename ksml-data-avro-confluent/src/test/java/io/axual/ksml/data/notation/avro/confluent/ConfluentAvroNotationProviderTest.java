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
import io.axual.ksml.data.notation.avro.AvroNotation;
import io.axual.ksml.data.notation.vendor.VendorNotation;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class ConfluentAvroNotationProviderTest {

    @Test
    @DisplayName("Provider exposes notation/vendor names for Confluent Avro")
    void providerMetadata_isCorrect() {
        assertThat(new ConfluentAvroNotationProvider())
                .returns(AvroNotation.NOTATION_NAME, ConfluentAvroNotationProvider::notationName)
                .returns("confluent", ConfluentAvroNotationProvider::vendorName);
    }

    @Test
    @DisplayName("createNotation wires AvroNotation with Confluent serde supplier (default ctor, no SR client)")
    void createNotation_defaultConstructor_wiresConfluentSerdeSupplier() {
        // Given a provider without an explicit Schema Registry client
        var provider = new ConfluentAvroNotationProvider();
        var context = new NotationContext(AvroNotation.NOTATION_NAME, "confluent");

        // Then
        assertThat(provider.createNotation(context))
                .asInstanceOf(InstanceOfAssertFactories.type(AvroNotation.class))
                .returns("confluent_" + AvroNotation.NOTATION_NAME, AvroNotation::name)
                .returns(".avsc", AvroNotation::filenameExtension)
                .returns(AvroNotation.DEFAULT_TYPE, AvroNotation::defaultType)
                .asInstanceOf(InstanceOfAssertFactories.type(VendorNotation.class))
                .extracting(VendorNotation::serdeSupplier)
                .asInstanceOf(InstanceOfAssertFactories.type(ConfluentAvroSerdeSupplier.class))
                .extracting(ConfluentAvroSerdeSupplier::registryClient)
                .isNull();
    }

    @Test
    @DisplayName("createNotation wires supplier with provided SchemaRegistryClient")
    void createNotation_withRegistryClient_propagatesClient() {
        // Given
        var registryClient = mock(SchemaRegistryClient.class);
        var provider = new ConfluentAvroNotationProvider(registryClient);
        var context = new NotationContext(AvroNotation.NOTATION_NAME, "confluent");

        assertThat(provider.createNotation(context))
                .asInstanceOf(InstanceOfAssertFactories.type(VendorNotation.class))
                .extracting(VendorNotation::serdeSupplier, InstanceOfAssertFactories.type(ConfluentAvroSerdeSupplier.class))
                .extracting(ConfluentAvroSerdeSupplier::registryClient)
                .isSameAs(registryClient);
    }
}
