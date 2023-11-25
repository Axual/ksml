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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public abstract class SchemaLoader implements SchemaLibrary.Loader {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaLoader.class);
    private static final String DIRECTORY_SEPARATOR = "/";
    private final String schemaDescr;
    private final String schemaDirectory;
    private final String schemaFileExtension;

    public SchemaLoader(String schemaDescr, String schemaDirectory, String fileExtension) {
        this.schemaDescr = schemaDescr;
        this.schemaDirectory = schemaDirectory + (schemaDirectory.endsWith(DIRECTORY_SEPARATOR) ? "" : DIRECTORY_SEPARATOR);
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
        LOG.warn("Could not find/load " + schemaDescr + " schema: " + schemaFile);
        return null;
    }

    protected abstract DataSchema parseSchema(String schemaName, String schema);
}
