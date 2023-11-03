package io.axual.ksml.data.schema;

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

import io.axual.ksml.exception.KSMLExecutionException;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class SchemaLibrary {
    private static final Map<String, Loader> loaders = new TreeMap<>();
    private static final Map<String, Map<String, NamedSchema>> schemas = new HashMap<>();

    public interface Loader {
        DataSchema load(String schemaName);
    }

    private SchemaLibrary() {
    }

    public static DataSchema getSchema(String schemaName, boolean allowNull) {
        // Look up the schema in the list of already loaded schemas. The process goes alphabetically, so any AVRO
        // schema automatically overrules any CSV schema with the same name. This process only applies for schema
        // types returned from Python. Any other schema references should know its notation and therefore those
        // lookups should not go through this method.
        for (var notationSchemas : schemas.entrySet()) {
            var notationSchema = notationSchemas.getValue().get(schemaName);
            if (notationSchema != null) return notationSchema;
        }

        if (!allowNull) {
            throw new KSMLExecutionException("Unknown schema: " + schemaName);
        }
        return null;
    }

    public static DataSchema getSchema(String notationName, String schemaName, boolean allowNull) {
        var notationSchemas = schemas.get(notationName);
        if (notationSchemas != null) {
            var schema = notationSchemas.get(schemaName);
            if (schema != null) return schema;
        }

        var loader = loaders.get(notationName);
        if (loader == null) return null;

        var schema = loader.load(schemaName);
        if (schema instanceof NamedSchema ns) {
            if (notationSchemas == null) {
                notationSchemas = new TreeMap<>();
                schemas.put(notationName, notationSchemas);
            }
            notationSchemas.put(schemaName, ns);
        }

        if (!allowNull && schema == null) {
            throw new KSMLExecutionException("Can not load " + (notationName != null ? notationName : "UNKNOWN") + " schema: " + schemaName);
        }
        return schema;
    }

    public static void registerLoader(String notationName, Loader loader) {
        loaders.put(notationName, loader);
    }
}
