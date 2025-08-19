package io.axual.ksml.data.notation.avro.confluent;

import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.type.DataType;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

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
        var supplier = new ConfluentAvroSerdeSupplier(new NotationContext("avro", "confluent"));

        // When
        Serde<Object> serde = supplier.get(DataType.UNKNOWN, false);

        // Then
        assertThat(serde).isInstanceOf(Serdes.WrapperSerde.class);
        assertThat(serde.serializer()).isInstanceOf(KafkaAvroSerializer.class);
        assertThat(serde.deserializer()).isInstanceOf(KafkaAvroDeserializer.class);
        assertThat(supplier.registryClient()).isNull();
    }

    @Test
    @DisplayName("get() returns WrapperSerde with KafkaAvroSerializer/Deserializer (with SR client)")
    void get_returnsSerdeWithConfluentSerdes_withClient() {
        // Given
        SchemaRegistryClient client = mock(SchemaRegistryClient.class);
        var supplier = new ConfluentAvroSerdeSupplier(new NotationContext("avro", "confluent"), client);

        // When
        Serde<Object> serde = supplier.get(DataType.UNKNOWN, true);

        // Then
        assertThat(serde).isInstanceOf(Serdes.WrapperSerde.class);
        assertThat(serde.serializer()).isInstanceOf(KafkaAvroSerializer.class);
        assertThat(serde.deserializer()).isInstanceOf(KafkaAvroDeserializer.class);
        // We can't inspect the client inside Confluent classes without reflection; instead, verify
        // the supplier exposes the provided client instance.
        assertThat(supplier.registryClient()).isSameAs(client);
    }
}
