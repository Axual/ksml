package io.axual.ksml.execution;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2023 Axual B.V.
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
import io.axual.ksml.data.notation.Notation;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.NamedSchema;
import io.axual.ksml.exception.ExecutionException;
import io.axual.ksml.schema.SchemaLoader;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.TreeMap;

@Slf4j
public class SchemaLibrary {
    private final Map<String, Map<String, NamedSchema>> schemas = new TreeMap<>();
    @Setter
    private String schemaDirectory = "";

    public DataSchema getSchema(String schemaName, boolean allowNull) {
        // Look up the schema in the list of already loaded schemas. The process goes alphabetically, so any AVRO
        // schema automatically overrules any CSV schema with the same name. This process only applies for schema
        // types returned from Python. Any other schema references should know its notation and therefore those
        // lookups should not go through this method.
        for (var notationSchemas : schemas.entrySet()) {
            var notationSchema = notationSchemas.getValue().get(schemaName);
            if (notationSchema != null) return notationSchema;
        }

        if (!allowNull) {
            throw new ExecutionException("Unknown schema: " + schemaName);
        }
        return null;
    }

    public DataSchema getSchema(Notation notation, String schemaName, boolean allowNull) {
        var notationSchemas = schemas.get(notation.name());
        if (notationSchemas != null) {
            var schema = notationSchemas.get(schemaName);
            if (schema != null) return schema;
        }

        if (notation.schemaParser() == null) return null;

        final var loader = new SchemaLoader(notation.name(), schemaDirectory, notation.filenameExtension());
        final var schemaStr = loader.load(schemaName);
        if (schemaStr != null) {
            final var schema = notation.schemaParser().parse(schemaName + notation.filenameExtension(), schemaName, schemaStr);
            if (schema instanceof NamedSchema ns) {
                if (notationSchemas == null) {
                    notationSchemas = new TreeMap<>();
                    schemas.put(notation.name(), notationSchemas);
                }
                notationSchemas.put(schemaName, ns);
            }
            if (schema != null) return schema;
        }

        if (allowNull) return null;
        throw new SchemaException("Can not load schema: notation=" + (notation.name() != null ? notation.name() : "null") + ", schema=" + schemaName);
    }

    /**
     * Gets a schema from the cache or fetches it from a remote schema registry.
     * The fetched schema is cached using the subject name for subsequent lookups.
     *
     * @param notation the notation to use for fetching
     * @param subject  the schema registry subject name (e.g., "my-topic-value")
     * @return the fetched and cached DataSchema
     */
    public DataSchema getOrFetchRemoteSchema(Notation notation, String subject) {
        var notationSchemas = schemas.get(notation.name());
        if (notationSchemas != null) {
            var schema = notationSchemas.get(subject);
            if (schema != null) {
                log.debug("Using cached remote schema for subject '{}'", subject);
                return schema;
            }
        }

        final var schema = notation.fetchRemoteSchema(subject);
        if (schema instanceof NamedSchema ns) {
            if (notationSchemas == null) {
                notationSchemas = new TreeMap<>();
                schemas.put(notation.name(), notationSchemas);
            }
            notationSchemas.put(subject, ns);
        }
        return schema;
    }
}
