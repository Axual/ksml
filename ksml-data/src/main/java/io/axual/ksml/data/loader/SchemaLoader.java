package io.axual.ksml.data.loader;

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


import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.SchemaLibrary;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

@Slf4j
public abstract class SchemaLoader implements SchemaLibrary.Loader {
    private final String schemaType;
    private final String schemaDirectory;
    private final String schemaFileExtension;

    public SchemaLoader(String schemaType, String schemaDirectory, String fileExtension) {
        this.schemaType = schemaType;
        this.schemaDirectory = schemaDirectory + (schemaDirectory.endsWith(File.separator) ? "" : File.separator);
        this.schemaFileExtension = fileExtension;
    }

    public DataSchema load(String schemaName) {
        // Load the schema with given (fully qualified) name
        while (true) {
            var result = loadInternal(schemaName);
            if (result != null) {
                return result;
            }
            if (!schemaName.contains(".")) break;
            schemaName = schemaName.substring(schemaName.indexOf(".") + 1);
        }

        return null;
    }

    private DataSchema loadInternal(String schemaName) {
        var schemaFile = schemaDirectory + schemaName + schemaFileExtension;

        // Try to load the schema as resource
        try {
            try (var resource = getClass().getResourceAsStream(schemaName)) {
                if (resource != null) {
                    var contents = new String(resource.readAllBytes(), StandardCharsets.UTF_8);
                    var schema = parseSchema(schemaName, contents);
                    if (schema != null) return schema;
                }
            }
        } catch (Exception e) {
            // Ignore
        }

        // Try to load the schema from file
        try {
            var contents = Files.readString(Path.of(schemaFile));
            var schema = parseSchema(schemaName, contents);
            if (schema != null) return schema;
        } catch (IOException e) {
            // Ignore
        }

        // Warn and exit
        log.warn("Could not find/load {} schema: {}", schemaType, schemaFile);
        return null;
    }

    protected abstract DataSchema parseSchema(String schemaName, String schema);
}
