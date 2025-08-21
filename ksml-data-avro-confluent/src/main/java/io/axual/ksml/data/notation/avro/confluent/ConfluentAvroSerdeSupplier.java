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

/**
 * Supplies Kafka {@code Serde} instances for Avro using Confluent's serializer/deserializer.
 * <p>
 * This class bridges KSML's Avro notation to Confluent's Schema Registry–backed serdes.
 * It implements {@link io.axual.ksml.data.notation.avro.AvroSerdeSupplier} and returns
 * {@link org.apache.kafka.common.serialization.Serde Serde}s composed of
 * {@link io.confluent.kafka.serializers.KafkaAvroSerializer} and
 * {@link io.confluent.kafka.serializers.KafkaAvroDeserializer}.
 * <p>
 * A {@link io.confluent.kafka.schemaregistry.client.SchemaRegistryClient} can optionally be
 * provided via the constructor. When present, it is used to construct the serializer and
 * deserializer, which is convenient for testing (mockable) and for applications that want
 * to reuse a managed client instance. When not provided, the default no-arg Confluent
 * serializer/deserializer constructors are used and will expect configuration via Kafka
 * client configs at runtime (eg. schema.registry.url, auth, etc.).
 */
public class ConfluentAvroSerdeSupplier implements AvroSerdeSupplier {
    private final NotationContext notationContext;
    // Registry Client is mocked by tests
    @Getter
    private final SchemaRegistryClient registryClient;

    /**
     * Creates a supplier using default Confluent serializer/deserializer instances.
     * The created serdes will rely on standard Kafka client configuration for Schema Registry
     * connectivity (for example, schema.registry.url).
     *
     * @param notationContext the notation context under which this supplier operates; kept for
     *                        future extension and parity with other suppliers
     */
    public ConfluentAvroSerdeSupplier(NotationContext notationContext) {
        this(notationContext, null);
    }

    /**
     * Creates a supplier using the provided {@link SchemaRegistryClient}. This allows tests to
     * pass a mock client and applications to provide a preconfigured client instance.
     *
     * @param notationContext the notation context under which this supplier operates
     * @param registryClient  the Schema Registry client to use for serializer/deserializer creation;
     *                        when {@code null} the default no-arg Confluent classes are used
     */
    public ConfluentAvroSerdeSupplier(NotationContext notationContext, SchemaRegistryClient registryClient) {
        this.notationContext = notationContext;
        this.registryClient = registryClient;
    }

    /**
     * Returns the vendor name used to namespace the Avro notation within KSML.
     * For Confluent-backed Avro this is always {@code "confluent"}.
     */
    @Override
    public String vendorName() {
        return "confluent";
    }

    /**
     * Returns a Kafka {@link Serde} for Avro values using Confluent's serializer and deserializer.
     * <p>
     * The returned Serde is a {@link org.apache.kafka.common.serialization.Serdes.WrapperSerde} wrapping
     * a {@link KafkaAvroSerializer} and a {@link KafkaAvroDeserializer}. If this supplier was created
     * with a non-null {@link SchemaRegistryClient}, that client is passed to both serializer and
     * deserializer constructors; otherwise their no-arg constructors are used.
     *
     * @param type  the logical KSML data type for which a serde is requested (not used directly here)
     * @param isKey whether this serde will be used for record keys (not used directly here)
     * @return a Serde<Object> backed by Confluent Avro serializer/deserializer
     */
    @Override
    public Serde<Object> get(DataType type, boolean isKey) {
        return Serdes.serdeFrom(
                registryClient != null ? new KafkaAvroSerializer(registryClient) : new KafkaAvroSerializer(),
                registryClient != null ? new KafkaAvroDeserializer(registryClient) : new KafkaAvroDeserializer());
    }
}
