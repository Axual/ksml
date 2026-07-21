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
import io.axual.ksml.data.notation.avro.RemoteSchemaAvroNotation;
import io.axual.ksml.data.notation.vendor.VendorNotationContext;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

/**
 * Confluent-backed AvroNotation that supports fetching schemas from a schema registry.
 * <p>
 * Extends {@link RemoteSchemaAvroNotation}, providing the Confluent-specific registry client and fetch.
 */
public class ConfluentAvroNotation extends RemoteSchemaAvroNotation {
    private final SchemaRegistryClient registryClient;

    public ConfluentAvroNotation(VendorNotationContext context, SchemaRegistryClient registryClient, Resolver topicResolver) {
        super(context, topicResolver::resolve);
        this.registryClient = registryClient;
    }

    @Override
    protected boolean hasRegistryClient() {
        return registryClient != null;
    }

    @Override
    protected String registryDescription() {
        return "Confluent schema registry";
    }

    @Override
    protected String fetchSchemaString(String subject) throws Exception {
        return registryClient.getLatestSchemaMetadata(subject).getSchema();
    }
}
