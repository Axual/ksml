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


import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import io.axual.ksml.generator.YAMLObjectMapper;
import io.axual.ksml.python.PythonContextConfig;
import io.axual.ksml.runner.config.internal.KsmlFileOrDefinition;
import io.axual.ksml.runner.exception.ConfigException;
import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.jackson.Jacksonized;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@JsonIgnoreProperties(ignoreUnknown = false)
@JsonClassDescription("""
        The `ksml` section contains all configuration specific to execute a KSML application.
        It controls where KSML looks for definitions and schemas, Notations, Kafka and schema registry settings as well as observability features.
        """)
@AllArgsConstructor
@NoArgsConstructor
@Jacksonized
@Data
public class KSMLConfig {
    public static final String DEFAULT_LOCATION = System.getProperty("user.dir");

    @JsonProperty(value = "applicationServer", required = false)
    @JsonPropertyDescription("Configures a REST API for state store queries and health checks")
    private ApplicationServerConfig applicationServerConfig = new ApplicationServerConfig();
    @JsonProperty(value = "prometheus", required = false)
    @JsonPropertyDescription("Configures a Prometheus metrics endpoint")
    private PrometheusConfig prometheusConfig = new PrometheusConfig();

    @NotBlank
    @JsonProperty(value = "configDirectory", required = false)
    @JsonPropertyDescription("Directory containing KSML definition files. Defaults to the working directory")
    private String configDirectory = DEFAULT_LOCATION;

    @NotBlank
    @JsonProperty(value = "schemaDirectory", required = false)
    @JsonPropertyDescription("Directory containing schema files. Defaults to the value of configDirectory")
    private String schemaDirectory = DEFAULT_LOCATION;

    @NotBlank
    @JsonProperty(value = "storageDirectory", required = false)
    @JsonPropertyDescription("Directory for Kafka Streams state stores. Defaults to the System temp directory")
    private String storageDirectory = DEFAULT_LOCATION;

    @JsonProperty(value = "createStorageDirectory", required = false)
    @JsonPropertyDescription("Create storage directory if it doesn't exist. Default value is false")
    private boolean createStorageDirectory = false;

    @JsonProperty(value = "enableProducers", required = false)
    @JsonPropertyDescription("Toggle to enable or disable the creation of producers in the KSML definitions. Default value is true")
    private boolean enableProducers = true;

    @JsonProperty(value = "enablePipelines", required = false)
    @JsonPropertyDescription("Toggle to enable or disable the creation of pipelines in the KSML definitions. Default value is true")
    private boolean enablePipelines = true;

    @JsonProperty(value = "errorHandling", required = false)
    @JsonPropertyDescription("Configures how different types of errors are handled")
    private ErrorHandlingConfig errorHandlingConfig = new ErrorHandlingConfig();
    @JsonProperty(value = "schemaRegistries", required = false)
    @JsonPropertyDescription("Configure named connections to schema registries. These can be referred to by notations which need access to a Schema Registry")
    private SchemaRegistryMap schemaRegistries;
    @JsonProperty(value = "notations", required = false)
    @JsonPropertyDescription("Configure named data format serializers and deserializers")
    private NotationMap notations;
    @JsonProperty(value = "definitions", required = true)
    @JsonPropertyDescription("Contains a map for KSML Definitions. The key is used as a namespace")
    private KsmlDefinitionMap definitions;
    @JsonProperty("pythonContext")
    @JsonPropertyDescription("Control Python execution security and permissions.")
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
        if(applicationServerConfig == null) {
            applicationServerConfig = new ApplicationServerConfig();
        }
        return applicationServerConfig;
    }

    public ErrorHandlingConfig errorHandlingConfig() {
        if (errorHandlingConfig == null) return new ErrorHandlingConfig();
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
            for (var definition : definitions.entrySet()) {
                var namespace = definition.getKey();
                var valueObj = definition.getValue();
                if (valueObj == null) continue;

                if (valueObj.getValue() instanceof String definitionFile) {
                    final var definitionFilePath = Paths.get(configDirectory(), definitionFile);
                    if (Files.notExists(definitionFilePath) || !Files.isRegularFile(definitionFilePath)) {
                        throw new ConfigException("definitionFile", definitionFilePath, "The provided KSML definition file does not exists or is not a regular file");
                    }
                    try {
                        log.info("Reading KSML definition from source file: {}", definitionFilePath.toFile());
                        final var def = YAMLObjectMapper.INSTANCE.readValue(definitionFilePath.toFile(), JsonNode.class);
                        result.put(namespace, def);
                    } catch (IOException e) {
                        log.error("Could not read KSML definition from file: {}", definitionFilePath);
                    }
                }
                if (valueObj.getValue() instanceof ObjectNode root) {
                    result.put(namespace, root);
                }
            }
        }
        return result;
    }

    /* Define Several Map Definitions to allow typed naming */
    @JsonClassDescription("Contains a map for KSML Definitions. The key is used as a namespace")
    @Data
    public static class KsmlDefinitionMap extends HashMap<String, KsmlFileOrDefinition> {

        @JsonCreator
        public KsmlDefinitionMap() {
            super();
        }

        @JsonAnySetter
        public void add(String property, KsmlFileOrDefinition value) {
            this.put(property, value);
        }

    }

    @JsonClassDescription("Configure named connections to schema registries. These can be referred to by notations which need access to a Schema Registry")
    @Data
    public static class SchemaRegistryMap extends HashMap<String, SchemaRegistryConfig> {
        @JsonCreator
        public SchemaRegistryMap() {
            super();
        }

        @JsonAnySetter
        @JsonCreator
        public void add(String property, SchemaRegistryConfig value) {
            this.put(property, value);
        }
    }

    @JsonClassDescription("Configure named data format serializers and deserializers")
    @Data
    public static class NotationMap extends HashMap<String, NotationConfig> {
        @JsonCreator
        public NotationMap() {
            super();
        }

        @JsonAnySetter
        public void add(String property, NotationConfig value) {
            this.put(property, value);
        }
    }
}
