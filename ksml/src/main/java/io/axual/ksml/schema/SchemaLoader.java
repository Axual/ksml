package io.axual.ksml.schema;

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
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

@Slf4j
public class SchemaLoader {
    private final String schemaType;
    private final String schemaDirectory;
    private final String schemaFileExtension;

    public SchemaLoader(String schemaType, String schemaDirectory, String fileExtension) {
        this.schemaType = schemaType;
        this.schemaDirectory = schemaDirectory + (schemaDirectory.isEmpty() || schemaDirectory.endsWith(File.separator) ? "" : File.separator);
        this.schemaFileExtension = fileExtension;
    }

    public String load(String schemaName, boolean allowNull) {
        final var result = load(schemaName);
        if (result == null && !allowNull) throw new SchemaException("Can not load schema: " + schemaName);
        return result;
    }

    public String load(String schemaName) {
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

    private String loadInternal(String schemaName) {
        var schemaFile = schemaDirectory + schemaName + schemaFileExtension;

        // Try to load the schema as resource
        try {
            try (final var resource = getClass().getResourceAsStream(schemaFile)) {
                if (resource != null) {
                    return new String(resource.readAllBytes(), StandardCharsets.UTF_8);
                }
            }
        } catch (Exception e) {
            // Ignore
        }

        // Try to load the schema from file
        try {
            return Files.readString(Path.of(schemaFile));
        } catch (IOException e) {
            // Ignore
        }

        // Warn and exit
        log.warn("Could not find/load {} schema: {}", schemaType, schemaFile);
        return null;
    }
}
