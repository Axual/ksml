package io.axual.ksml.data.notation.avro;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - AVRO
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

import io.axual.ksml.data.exception.SchemaException;
import io.axual.ksml.data.notation.vendor.VendorNotationContext;
import io.axual.ksml.data.schema.DataSchema;
import lombok.extern.slf4j.Slf4j;

import java.util.function.UnaryOperator;

/**
 * Base for vendor-backed {@link AvroNotation}s that fetch schemas from a schema registry.
 *
 * <p>It implements the common remote-schema flow (resolve the topic to a subject, fetch the schema string,
 * parse it) and defers the vendor-specific parts to subclasses: whether a client is configured, the
 * registry name used in the warning, and the actual per-subject fetch. The topic resolver is taken as a
 * {@link UnaryOperator} so this base stays free of the Kafka-client dependency.</p>
 */
@Slf4j
public abstract class RemoteSchemaAvroNotation extends AvroNotation {
    private final UnaryOperator<String> topicResolver;

    protected RemoteSchemaAvroNotation(VendorNotationContext context, UnaryOperator<String> topicResolver) {
        super(context);
        this.topicResolver = topicResolver;
    }

    /** Whether a schema registry client is configured. */
    protected abstract boolean hasRegistryClient();

    /** Human-readable registry name for the "not configured" warning, e.g. "Apicurio registry". */
    protected abstract String registryDescription();

    /** Fetch the raw schema string for the given subject from the registry. */
    protected abstract String fetchSchemaString(String subject) throws Exception;

    @Override
    public boolean supportsRemoteSchema() {
        if (hasRegistryClient()) return true;
        log.warn("{} not configured, remote schema resolution is disabled", registryDescription());
        return false;
    }

    @Override
    public DataSchema fetchRemoteSchema(String topic, boolean isKey) {
        if (!hasRegistryClient()) {
            throw new SchemaException("Cannot fetch remote schema: no schema registry client configured");
        }

        final var subject = topicResolver.apply(topic) + (isKey ? "-key" : "-value");
        try {
            log.info("Fetching latest schema for subject '{}' from schema registry", subject);
            return schemaParser().parse(subject, subject, fetchSchemaString(subject));
        } catch (Exception e) {
            throw new SchemaException("Failed to fetch schema for subject '" + subject + "' from schema registry", e);
        }
    }
}
