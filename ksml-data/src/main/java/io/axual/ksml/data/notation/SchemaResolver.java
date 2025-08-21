package io.axual.ksml.data.notation;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
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

import io.axual.ksml.data.exception.SchemaException;
import io.axual.ksml.data.schema.DataSchema;

/**
 * Resolves schemas by name. Implementations typically look up parsed DataSchema instances
 * from a registry, cache, or local collection.
 *
 * @param <T> the type of DataSchema handled by the resolver
 */
public interface SchemaResolver<T extends DataSchema> extends ReferenceResolver<T> {
    /**
     * Returns the schema for the given name or throws when it is not found.
     *
     * @param schemaName the schema name
     * @return the resolved schema
     * @throws SchemaException when the schema is unknown
     */
    default T getOrThrow(String schemaName) {
        final var result = get(schemaName);
        if (result == null) throw new SchemaException("Unknown schema: " + schemaName);
        return result;
    }
}
