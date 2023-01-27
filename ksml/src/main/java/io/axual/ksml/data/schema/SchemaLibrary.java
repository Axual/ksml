package io.axual.ksml.data.schema;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 Axual B.V.
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

public class SchemaLibrary {
    private static final Map<String, Loader> loaders = new HashMap<>();
    private static final Map<String, NamedSchema> schemas = new HashMap<>();

    public interface Loader {
        DataSchema load(String schemaName);
    }

    private SchemaLibrary() {
    }

    public static DataSchema getSchema(String schemaName, boolean allowNull) {
        for (String notationName : loaders.keySet()) {
            var schema = getSchema(notationName, schemaName);
            if (schema != null) return schema;
        }

        if (!allowNull) {
            throw new KSMLExecutionException("Can not load schema: " + schemaName);
        }
        return null;
    }

    public static DataSchema getSchema(String notationName, String schemaName, boolean allowNull) {
        var result = getSchema(notationName, schemaName);
        if (result == null && !allowNull) {
            throw new KSMLExecutionException("Can not load schema: " + notationName + ":" + schemaName);
        }
        return result;
    }

    private static DataSchema getSchema(String notationName, String schemaName) {
        if (schemas.containsKey(schemaName)) {
            return schemas.get(schemaName);
        }
        var loader = loaders.get(notationName);
        if (loader == null) return null;

        var schema = loader.load(schemaName);
        if (schema instanceof NamedSchema ns) {
            schemas.put(schemaName, ns);
        }
        return schema;
    }

    public static void registerLoader(String notationName, Loader loader) {
        loaders.put(notationName, loader);
    }
}
