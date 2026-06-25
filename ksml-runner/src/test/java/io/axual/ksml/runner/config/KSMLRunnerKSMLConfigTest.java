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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.axual.ksml.python.PythonContextConfig;
import io.axual.ksml.runner.config.internal.KsmlFilePath;
import io.axual.ksml.runner.config.internal.KsmlInlineDefinition;
import io.axual.ksml.runner.exception.ConfigException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KSMLRunnerKSMLConfigTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setup() {
        objectMapper = new ObjectMapper(new YAMLFactory());
    }

    @Test
    @DisplayName("validate of complete config should not throw exceptions")
    void shouldValidateConfig() throws Exception {
        final var yaml = getClass().getClassLoader().getResourceAsStream("ksml-config.yaml");
        final var ksmlConfig = objectMapper.readValue(yaml, KSMLConfig.class);
        assertNotNull(ksmlConfig.configDirectory());
    }

    @Test
    @DisplayName("validate of incorrect config directory should throw exception")
    void shouldThrowOnWrongConfigdir() throws Exception {
        final var yaml = getClass().getClassLoader().getResourceAsStream("ksml-config-wrong-configdir.yaml");
        final var ksmlConfig = objectMapper.readValue(yaml, KSMLConfig.class);
        assertThrows(ConfigException.class, ksmlConfig::configDirectory, "should throw exception for wrong configdir");
    }

    @Test
    @DisplayName("if configdir is missing it should default to workdir")
    void shouldDefaultConfigToWorkdir() throws Exception {
        final var yaml = getClass().getClassLoader().getResourceAsStream("ksml-config-no-configdir.yaml");
        final var ksmlConfig = objectMapper.readValue(yaml, KSMLConfig.class);
        assertEquals(System.getProperty("user.dir"), ksmlConfig.configDirectory(), "config dir should default to working dir");
    }

    @Test
    @DisplayName("schemaDirectory and storageDirectory resolve from a valid config")
    void shouldResolveSchemaAndStorageDirectories() throws Exception {
        final var yaml = getClass().getClassLoader().getResourceAsStream("ksml-config.yaml");
        final var ksmlConfig = objectMapper.readValue(yaml, KSMLConfig.class);
        // schemaDirectory is not set in the yaml, so it defaults to the working directory
        assertEquals(System.getProperty("user.dir"), ksmlConfig.schemaDirectory(), "schema dir should default to working dir");
        // storageDirectory is set to "." in the yaml, which resolves to the working directory
        assertEquals(System.getProperty("user.dir"), ksmlConfig.storageDirectory(), "storage dir '.' should resolve to working dir");
    }

    @Test
    @DisplayName("storageDirectory is created when createStorageDirectory is true")
    void shouldCreateStorageDirectory(@TempDir Path tempDir) {
        final var newDir = tempDir.resolve("state-store");
        final var ksmlConfig = new KSMLConfig();
        ksmlConfig.storageDirectory(newDir.toString());
        ksmlConfig.createStorageDirectory(true);

        assertEquals(newDir.toAbsolutePath().normalize().toString(), ksmlConfig.storageDirectory());
        assertTrue(Files.isDirectory(newDir), "storage directory should have been created");
    }

    @Test
    @DisplayName("notations() and schemaRegistries() default to empty maps when nothing is configured")
    void collectionAccessorsDefaultToEmpty() {
        final var ksmlConfig = new KSMLConfig();
        assertTrue(ksmlConfig.notations().isEmpty(), "notations should default to an empty map");
        assertTrue(ksmlConfig.schemaRegistries().isEmpty(), "schemaRegistries should default to an empty map");
        assertTrue(ksmlConfig.definitions().isEmpty(), "definitions should default to an empty map");
    }

    @Test
    @DisplayName("notations() exposes the configured entries as an unmodifiable map")
    void notationsAreExposedReadOnly() {
        final var notations = new KSMLConfig.NotationMap();
        notations.add("myAvro", NotationConfig.builder().type(NotationConfig.NotationType.CONFLUENT_AVRO).build());
        final var ksmlConfig = new KSMLConfig();
        ksmlConfig.notations(notations);

        assertEquals(1, ksmlConfig.notations().size());
        assertEquals(NotationConfig.NotationType.CONFLUENT_AVRO, ksmlConfig.notations().get("myAvro").type());
        assertThrows(UnsupportedOperationException.class,
                () -> ksmlConfig.notations().clear(),
                "the accessor must return a read-only view");
    }

    @Test
    @DisplayName("schemaRegistries() exposes the configured entries as an unmodifiable map")
    void schemaRegistriesAreExposedReadOnly() {
        final var registries = new KSMLConfig.SchemaRegistryMap();
        registries.add("primary", SchemaRegistryConfig.builder().build());
        final var ksmlConfig = new KSMLConfig();
        ksmlConfig.schemaRegistries(registries);

        assertEquals(1, ksmlConfig.schemaRegistries().size());
        assertTrue(ksmlConfig.schemaRegistries().containsKey("primary"));
        assertThrows(UnsupportedOperationException.class,
                () -> ksmlConfig.schemaRegistries().clear(),
                "the accessor must return a read-only view");
    }

    @Test
    @DisplayName("definitions() returns inline definitions unchanged")
    void definitionsReturnInlineDefinitions() {
        final ObjectNode inline = objectMapper.createObjectNode().put("hello", "world");
        final var defs = new KSMLConfig.KsmlDefinitionMap();
        defs.add("inlineNamespace", new KsmlInlineDefinition(inline));
        final var ksmlConfig = new KSMLConfig();
        ksmlConfig.definitions(defs);

        final var result = ksmlConfig.definitions();
        assertEquals(1, result.size());
        assertSame(inline, result.get("inlineNamespace"));
    }

    @Test
    @DisplayName("definitions() loads a definition referenced by a file path")
    void definitionsLoadFromFile() {
        final var defs = new KSMLConfig.KsmlDefinitionMap();
        defs.add("fileNamespace", new KsmlFilePath("load-definition-test.yaml"));
        final var ksmlConfig = new KSMLConfig();
        // The file path is resolved relative to the configDirectory.
        ksmlConfig.configDirectory("src/test/resources");
        ksmlConfig.definitions(defs);

        final var result = ksmlConfig.definitions();
        final var loaded = result.get("fileNamespace");
        assertNotNull(loaded, "the definition file should have been read");
        // The YAML content is parsed into a JsonNode tree we can inspect.
        assertTrue(loaded.path("streams").has("some_stream"), "the parsed definition should contain the stream");
    }

    @Test
    @DisplayName("definitions() throws when a referenced file does not exist")
    void definitionsThrowOnMissingFile() {
        final var defs = new KSMLConfig.KsmlDefinitionMap();
        defs.add("missing", new KsmlFilePath("does-not-exist.yaml"));
        final var ksmlConfig = new KSMLConfig();
        ksmlConfig.configDirectory("src/test/resources");
        ksmlConfig.definitions(defs);

        assertThrows(ConfigException.class, ksmlConfig::definitions,
                "a missing definition file should fail fast");
    }

    @Test
    @DisplayName("pythonContextConfig() falls back to a default when none is configured")
    void pythonContextConfigDefaults() {
        final var ksmlConfig = new KSMLConfig();
        assertNotNull(ksmlConfig.pythonContextConfig(), "a default python context config should be provided");

        final var explicit = PythonContextConfig.builder().build();
        ksmlConfig.pythonContextConfig(explicit);
        assertSame(explicit, ksmlConfig.pythonContextConfig(), "an explicit config should be returned as-is");
    }

    @Test
    @DisplayName("errorHandlingConfig() and applicationServerConfig() recover from a null field")
    void configAccessorsRecreateNullFields() {
        final var ksmlConfig = new KSMLConfig();
        ksmlConfig.errorHandlingConfig(null);
        ksmlConfig.applicationServerConfig(null);

        assertNotNull(ksmlConfig.errorHandlingConfig(), "a fresh error handling config should be returned");
        final var appServer = ksmlConfig.applicationServerConfig();
        assertNotNull(appServer, "a fresh application server config should be returned");
        assertFalse(appServer.enabled(), "the recreated application server should be disabled by default");
    }
}
