package io.axual.ksml.runner.config;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
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


import io.axual.ksml.runner.exception.ConfigException;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

@Slf4j
@Data
public class KSMLConfig {
    private static final String DEFAULT_HOSTNAME = "0.0.0.0";
    private static final String DEFAULT_PORT = "8080";

    private ApplicationServerConfig applicationServer;
    private String configDirectory;
    private String schemaDirectory;
    private String storageDirectory;
    private KSMLErrorHandlingConfig errorHandling;

    private List<String> definitions;

    public KSMLErrorHandlingConfig getErrorHandlingConfig() {
        if (errorHandling == null) return new KSMLErrorHandlingConfig();
        return errorHandling;
    }

    public void validate() throws ConfigException {
        configDirectory = configDirectory != null ? configDirectory : System.getProperty("user.dir");
        schemaDirectory = schemaDirectory != null ? schemaDirectory : configDirectory;
        storageDirectory = storageDirectory != null ? storageDirectory : System.getProperty("java.io.tmpdir");

        final var configPath = Paths.get(configDirectory);
        if (Files.notExists(configPath) || !Files.isDirectory(configPath)) {
            throw new ConfigException("config.directory", configDirectory, "The provided config path does not exist or is not a directory");
        }
        configDirectory = configPath.toAbsolutePath().normalize().toString();

        final var schemaPath = Paths.get(schemaDirectory);
        if (Files.notExists(schemaPath) || !Files.isDirectory(schemaPath)) {
            throw new ConfigException("schema.directory", schemaDirectory, "The provided schema path does not exist or is not a directory");
        }
        schemaDirectory = schemaPath.toAbsolutePath().normalize().toString();

        final var storagePath = Paths.get(storageDirectory);
        if (Files.notExists(storagePath) || !Files.isDirectory(storagePath)) {
            throw new ConfigException("storage.directory", storagePath, "The provided storage path does not exist or is not a directory");
        }
        storageDirectory = storagePath.toAbsolutePath().normalize().toString();

        if (definitions == null || definitions.isEmpty()) {
            throw new ConfigException("definitionFile", definitions, "At least one KSML definition file must be specified");
        }

        log.info("Using directories: config: {}, schema: {}, storage: {}", configDirectory, schemaDirectory, storageDirectory);

        for (String definitionFile : definitions) {
            final var definitionFilePath = Paths.get(getConfigDirectory(), definitionFile);
            if (Files.notExists(definitionFilePath) || !Files.isRegularFile(definitionFilePath)) {
                throw new ConfigException("definitionFile", definitionFilePath, "The provided KSML definition file does not exists or is not a regular file");
            }
        }
    }
}