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


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.axual.ksml.data.util.JsonNodeUtil;
import io.axual.ksml.generator.YAMLObjectMapper;
import io.axual.ksml.python.PythonContextConfig;
import io.axual.ksml.runner.exception.ConfigException;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@JsonIgnoreProperties(ignoreUnknown = true)
@Builder
@Jacksonized
public class KSMLConfig {
    private static final PrometheusConfig DEFAULT_PROMETHEUS_CONFIG = PrometheusConfig.builder()
            .enabled(false)
            .build();
    private static final ApplicationServerConfig DEFAULT_APPSERVER_CONFIG = ApplicationServerConfig.builder()
            .enabled(false)
            .build();

    @JsonProperty("applicationServer")
    @Builder.Default
    private ApplicationServerConfig applicationServerConfig = DEFAULT_APPSERVER_CONFIG;
    @JsonProperty("prometheus")
    @Builder.Default
    @Getter
    private PrometheusConfig prometheusConfig = DEFAULT_PROMETHEUS_CONFIG;

    private String configDirectory;
    private String schemaDirectory;
    private String storageDirectory;

    @Builder.Default
    private boolean createStorageDirectory = false;

    @Getter
    @Builder.Default
    private boolean enableProducers = true;
    @Getter
    @Builder.Default
    private boolean enablePipelines = true;

    @JsonProperty("errorHandling")
    private ErrorHandlingConfig errorHandlingConfig;
    @JsonProperty("schemaRegistries")
    private Map<String, SchemaRegistryConfig> schemaRegistries;
    @JsonProperty("notations")
    private Map<String, NotationConfig> notations;
    @JsonProperty("definitions")
    private Map<String, Object> definitions;
    @JsonProperty("schemas")
    private Map<String, Object> schemas;
    @JsonProperty("pythonContext")
    private PythonContextConfig pythonContextConfig;

    public PythonContextConfig pythonContextConfig() {
        return pythonContextConfig != null
                ? pythonContextConfig
                : PythonContextConfig.builder().build();
    }

    private String getDirectory(String configVariable, String directory) {
        return getDirectory(configVariable, directory, false);
    }

    private String getDirectory(String configVariable, String directory, boolean createIfNotExist) {
        final var configPath = Paths.get(directory);
        if (Files.notExists(configPath)) {
            if (createIfNotExist) {
                log.info("Directory {} does not exists, creating ", configPath);
                try {
                    Files.createDirectories(configPath);
                } catch (IOException e) {
                    throw new ConfigException(configVariable, directory, "Could not create the directory", e);
                }
            } else {
                throw new ConfigException(configVariable, directory, "The provided path does not exist");
            }
        }
        if (!Files.isDirectory(configPath)) {
            throw new ConfigException(configVariable, directory, "The provided path is not a directory");
        }
        return configPath.toAbsolutePath().normalize().toString();
    }

    public String configDirectory() {
        return getDirectory("configDirectory", configDirectory != null ? configDirectory : System.getProperty("user.dir"));
    }

    public String schemaDirectory() {
        return getDirectory("schemaDirectory", schemaDirectory != null ? schemaDirectory : configDirectory());
    }

    public String storageDirectory() {
        return getDirectory("storageDirectory", storageDirectory != null ? storageDirectory : System.getProperty("java.io.tmpdir"), createStorageDirectory);
    }

    public ApplicationServerConfig applicationServerConfig() {
        return applicationServerConfig;
    }

    public ErrorHandlingConfig errorHandlingConfig() {
        if (errorHandlingConfig == null) return ErrorHandlingConfig.builder().build();
        return errorHandlingConfig;
    }

    public Map<String, SchemaRegistryConfig> schemaRegistries() {
        if (schemaRegistries != null) return Collections.unmodifiableMap(schemaRegistries);
        return ImmutableMap.of();
    }

    public Map<String, NotationConfig> notations() {
        if (notations != null) return Collections.unmodifiableMap(notations);
        return ImmutableMap.of();
    }

    public Map<String, JsonNode> definitions() {
        final var result = new HashMap<String, JsonNode>();
        if (definitions != null) {
            for (Map.Entry<String, Object> definition : definitions.entrySet()) {
                if (definition.getValue() instanceof String definitionFile) {
                    final var definitionFilePath = Paths.get(configDirectory(), definitionFile);
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
                    final var root = JsonNodeUtil.convertNativeToJsonNode(definitionMap);
                    result.put(definition.getKey(), root);
                }
            }
        }
        return result;
    }
}