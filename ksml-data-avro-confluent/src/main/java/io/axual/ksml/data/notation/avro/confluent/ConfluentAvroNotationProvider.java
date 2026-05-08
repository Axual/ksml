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

import io.axual.ksml.client.resolving.ResolvingClientConfig;
import io.axual.ksml.data.notation.Notation;
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.avro.AvroDataObjectMapper;
import io.axual.ksml.data.notation.avro.AvroNotation;
import io.axual.ksml.data.notation.vendor.VendorNotationContext;
import io.axual.ksml.data.notation.vendor.VendorNotationProvider;
import io.axual.ksml.data.util.MapUtil;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Notation provider for Confluent-backed Avro support.
 *
 * <p>This provider implements the wiring between KSML's AvroNotation and Confluent's
 * Schema Registry–based serdes. It constructs an AvroNotation using a VendorNotationContext
 * that supplies:
 * - a ConfluentAvroSerdeSupplier for creating Kafka Serde pairs; and
 * - an AvroDataObjectMapper for Avro ↔ KSML DataObject conversion.
 *
 * <p>See also:
 * - ksml-data/DEVELOPER_GUIDE.md for Notation/Vendor wiring concepts
 * - ksml-data-avro/DEVELOPER_GUIDE.md for AvroNotation behavior and mappers
 */
public class ConfluentAvroNotationProvider extends VendorNotationProvider {
    private final SchemaRegistryClient registryClient;

    /**
     * Creates a provider without a pre-supplied Schema Registry client. The underlying
     * Confluent serializers/deserializers will use their default constructors and expect
     * configuration via the NotationContext serde configs at runtime.
     */
    public ConfluentAvroNotationProvider() {
        this(null);
    }

    /**
     * Creates a provider that will pass a specific SchemaRegistryClient to the Confluent
     * serializer/deserializer instances. This is useful in tests or environments where
     * a managed client instance is required.
     *
     * @param registryClient optional preconfigured Confluent Schema Registry client
     */
    public ConfluentAvroNotationProvider(SchemaRegistryClient registryClient) {
        super(AvroNotation.NOTATION_NAME, "confluent");
        this.registryClient = registryClient;
    }

    /**
     * Builds an AvroNotation instance backed by Confluent serdes using the given context.
     *
     * @param context the base notation context carrying name, vendor, native mapper and configs
     * @return an AvroNotation wired with ConfluentAvroSerdeSupplier and AvroDataObjectMapper
     */
    @Override
    public Notation createNotation(NotationContext context) {
        final Map<String, Object> serdeConfigs = context != null ? MapUtil.stringKeys(context.serdeConfigs()) : new HashMap<>();
        final var clientConfig = new ResolvingClientConfig(serdeConfigs);
        final var srClient = registryClient != null ? registryClient : createSrClient(serdeConfigs);
        return new ConfluentAvroNotation(
                new VendorNotationContext(
                        vendorName(),
                        context,
                        new ConfluentAvroSerdeSupplier(srClient),
                        new AvroDataObjectMapper()),
                srClient,
                clientConfig.topicResolver()
        );
    }

    private SchemaRegistryClient createSrClient(Map<String, Object> serdeConfigs) {
        if (!serdeConfigs.containsKey(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG)) return null;
        final var config = new KafkaAvroSerializerConfig(serdeConfigs);
        return SchemaRegistryClientFactory.newClient(
                config.getSchemaRegistryUrls(),
                config.getMaxSchemasPerSubject(),
                Collections.singletonList(new AvroSchemaProvider()),
                config.originalsWithPrefix(""),
                config.requestHeaders());
    }
}
