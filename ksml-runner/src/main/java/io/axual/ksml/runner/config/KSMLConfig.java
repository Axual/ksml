package io.axual.ksml.runner.config;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
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


import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import io.axual.ksml.generator.YAMLObjectMapper;
import io.axual.ksml.notation.binary.JsonNodeNativeMapper;
import io.axual.ksml.runner.exception.ConfigException;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class KSMLConfig {
    private static final String DEFAULT_HOSTNAME = "0.0.0.0";
    private static final String DEFAULT_PORT = "8080";

    @JsonProperty("applicationServer")
    private ApplicationServerConfig applicationServer;
    private String configDirectory;
    private String schemaDirectory;
    private String storageDirectory;

    @JsonProperty("errorHandling")
    private KSMLErrorHandlingConfig errorHandling;
    @JsonProperty("definitions")
    private Map<String, Object> definitions;

    private String getDirectory(String configVariable, String directory) {
        final var configPath = Paths.get(directory);
        if (Files.notExists(configPath) || !Files.isDirectory(configPath)) {
            throw new ConfigException(configVariable, directory, "The provided path does not exist or is not a directory");
        }
        return configPath.toAbsolutePath().normalize().toString();
    }

    public String getConfigDirectory() {
        return getDirectory("configDirectory", configDirectory != null ? configDirectory : System.getProperty("user.dir"));
    }

    public String getSchemaDirectory() {
        return getDirectory("schemaDirectory", schemaDirectory != null ? schemaDirectory : getConfigDirectory());
    }

    public String getStorageDirectory() {
        return getDirectory("storageDirectory", storageDirectory != null ? storageDirectory : System.getProperty("java.io.tmpdir"));
    }

    public ApplicationServerConfig getApplicationServerConfig() {
        return applicationServer;
    }

    public KSMLErrorHandlingConfig getErrorHandlingConfig() {
        if (errorHandling == null) return new KSMLErrorHandlingConfig();
        return errorHandling;
    }

    public Map<String, JsonNode> getDefinitions() {
        final var result = new HashMap<String, JsonNode>();
        for (Map.Entry<String, Object> definition : definitions.entrySet()) {
            if (definition.getValue() instanceof String definitionFile) {
                final var definitionFilePath = Paths.get(configDirectory, definitionFile);
                if (Files.notExists(definitionFilePath) || !Files.isRegularFile(definitionFilePath)) {
                    throw new ConfigException("definitionFile", definitionFilePath, "The provided KSML definition file does not exists or is not a regular file");
                }
                try {
                    log.info("Reading KSML definition from source file: {}", definitionFilePath.toFile());
                    final var def = YAMLObjectMapper.INSTANCE.readValue(definitionFilePath.toFile(), JsonNode.class);
                    result.put(definition.getKey(), def);
                } catch (IOException e) {
                    log.error("Could not read KSML definition from file: {}", definitionFilePath);
                }
            }
            if (definition.getValue() instanceof Map<?, ?> definitionMap) {
                final var mapper = new JsonNodeNativeMapper();
                final var root = mapper.fromNative(definitionMap);
                result.put(definition.getKey(), root);
            }
        }
        return result;
    }

    public void validate() throws ConfigException {

        log.info("Using directories: config: {}, schema: {}, storage: {}", configDirectory, schemaDirectory, storageDirectory);
    }
}