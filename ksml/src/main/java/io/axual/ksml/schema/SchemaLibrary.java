package io.axual.ksml.schema;

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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.axual.ksml.exception.KSMLExecutionException;

public class SchemaLibrary {
    private static final List<Loader> loaders = new ArrayList<>();
    private static final Map<String, DataSchema> schemas = new HashMap<>();

    public interface Loader {
        DataSchema load(String schemaName);
    }

    private SchemaLibrary() {
    }

    public static DataSchema getSchema(String schemaName) {
        if (schemas.containsKey(schemaName)) {
            return schemas.get(schemaName);
        }
        for (Loader loader : loaders) {
            DataSchema schema = loader.load(schemaName);
            if (schema != null) {
                schemas.put(schemaName, schema);
                return schema;
            }
        }
        return null;
    }

    public static void registerLoader(Loader loader) {
        loaders.add(loader);
    }

    public static void registerSchema(DataSchema schema) {
        if (!schemas.containsKey(schema.name())) {
            schemas.put(schema.name(), schema);
            return;
        }
        DataSchema existingSchema = schemas.get(schema.name());
        if (!existingSchema.equals(schema)) {
            throw new KSMLExecutionException("Encountered two different schema with the same name: " + schema.name());
        }
    }
}
