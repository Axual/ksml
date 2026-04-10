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

import io.axual.ksml.client.resolving.Resolver;
import io.axual.ksml.data.exception.SchemaException;
import io.axual.ksml.data.notation.avro.AvroNotation;
import io.axual.ksml.data.notation.vendor.VendorNotationContext;
import io.axual.ksml.data.schema.DataSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import lombok.extern.slf4j.Slf4j;

/**
 * Confluent-backed AvroNotation that supports fetching schemas from a schema registry.
 * <p>
 * This extends AvroNotation to override {@link #fetchRemoteSchema(String, boolean)}, enabling
 * deferred schema resolution for stream definitions that omit an explicit schema name.
 */
@Slf4j
public class ConfluentAvroNotation extends AvroNotation {
    private final SchemaRegistryClient registryClient;
    private final Resolver topicResolver;

    public ConfluentAvroNotation(VendorNotationContext context, SchemaRegistryClient registryClient, Resolver topicResolver) {
        super(context);
        this.registryClient = registryClient;
        this.topicResolver = topicResolver;
    }

    @Override
    public boolean supportsRemoteSchema() {
        if (registryClient != null) return true;
        log.warn("Confluent schema registry not configured, remote schema resolution is disabled");
        return false;
    }

    @Override
    public DataSchema fetchRemoteSchema(String topic, boolean isKey) {
        if (registryClient == null) {
            throw new SchemaException("Cannot fetch remote schema: no schema registry client configured");
        }

        final var subject = topicResolver.resolve(topic) + (isKey ? "-key" : "-value");
        try {
            log.info("Fetching latest schema for subject '{}' from schema registry", subject);
            final var metadata = registryClient.getLatestSchemaMetadata(subject);
            final var schemaString = metadata.getSchema();
            return schemaParser().parse(subject, subject, schemaString);
        } catch (Exception e) {
            throw new SchemaException("Failed to fetch schema for subject '" + subject + "' from schema registry", e);
        }
    }
}
