package io.axual.ksml.data.notation.avro.confluent;

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
