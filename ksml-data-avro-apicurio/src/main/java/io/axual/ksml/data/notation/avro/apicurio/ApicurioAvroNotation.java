package io.axual.ksml.data.notation.avro.apicurio;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - AVRO Apicurio
 * %%
 * Copyright (C) 2021 - 2026 Axual B.V.
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

import io.apicurio.registry.rest.client.RegistryClient;
import io.axual.ksml.data.exception.SchemaException;
import io.axual.ksml.data.notation.avro.AvroNotation;
import io.axual.ksml.data.notation.vendor.VendorNotationContext;
import io.axual.ksml.data.schema.DataSchema;
import lombok.extern.slf4j.Slf4j;

/**
 * Apicurio-backed AvroNotation that supports fetching schemas from a schema registry.
 * <p>
 * This extends AvroNotation to override {@link #fetchRemoteSchema(String)}, enabling
 * deferred schema resolution for stream definitions that omit an explicit schema name.
 */
@Slf4j
public class ApicurioAvroNotation extends AvroNotation {
    private final RegistryClient registryClient;

    /**
     * Construct an AvroNotation with the provided vendor context.
     *
     * @param context the vendor notation context providing serde supplier, native mapper, and configs
     */
    public ApicurioAvroNotation(VendorNotationContext context, RegistryClient registryClient) {
        super(context);
        this.registryClient = registryClient;
    }

    @Override
    public boolean supportsRemoteSchema() {
        return registryClient != null;
    }

    @Override
    public DataSchema fetchRemoteSchema(String subject) {
        if (registryClient == null) {
            throw new SchemaException("Cannot fetch remote schema: no schema registry client configured");
        }
        try {
            log.info("Fetching latest schema for subject '{}' from schema registry", subject);
            final var schema = registryClient.getLatestArtifact(null, subject);
            return schemaParser().parse(subject, subject, schema.toString());
        } catch (Exception e) {
            throw new SchemaException("Failed to fetch schema for subject '" + subject + "' from schema registry", e);
        }
    }
}
