package io.axual.ksml.runner.config;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
 * %%
 * Copyright (C) 2021 - 2026 Axual B.V.
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

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;
import tools.jackson.dataformat.yaml.YAMLFactory;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        assertThat(ksmlConfig.configDirectory()).isNotNull();
    }

    @Test
    @DisplayName("validate of incorrect config directory should throw exception")
    void shouldThrowOnWrongConfigdir() throws Exception {
        final var yaml = getClass().getClassLoader().getResourceAsStream("ksml-config-wrong-configdir.yaml");
        final var ksmlConfig = objectMapper.readValue(yaml, KSMLConfig.class);
        assertThatThrownBy(ksmlConfig::configDirectory)
                .as("should throw exception for wrong configdir")
                .isInstanceOf(ConfigException.class);
    }

    @Test
    @DisplayName("if configdir is missing it should default to workdir")
    void shouldDefaultConfigToWorkdir() throws Exception {
        final var yaml = getClass().getClassLoader().getResourceAsStream("ksml-config-no-configdir.yaml");
        final var ksmlConfig = objectMapper.readValue(yaml, KSMLConfig.class);
        assertThat(ksmlConfig.configDirectory()).as("config dir should default to working dir").isEqualTo(System.getProperty("user.dir"));
    }

    @Test
    @DisplayName("schemaDirectory and storageDirectory resolve from a valid config")
    void shouldResolveSchemaAndStorageDirectories() throws Exception {
        final var yaml = getClass().getClassLoader().getResourceAsStream("ksml-config.yaml");
        final var ksmlConfig = objectMapper.readValue(yaml, KSMLConfig.class);
        // schemaDirectory is not set in the yaml, so it defaults to the working directory
        assertThat(ksmlConfig.schemaDirectory()).as("schema dir should default to working dir").isEqualTo(System.getProperty("user.dir"));
        // storageDirectory is set to "." in the yaml, which resolves to the working directory
        assertThat(ksmlConfig.storageDirectory()).as("storage dir '.' should resolve to working dir").isEqualTo(System.getProperty("user.dir"));
    }

    @Test
    @DisplayName("storageDirectory is created when createStorageDirectory is true")
    void shouldCreateStorageDirectory(@TempDir Path tempDir) {
        final var newDir = tempDir.resolve("state-store");
        final var ksmlConfig = new KSMLConfig();
        ksmlConfig.storageDirectory(newDir.toString());
        ksmlConfig.createStorageDirectory(true);

        assertThat(ksmlConfig.storageDirectory()).isEqualTo(newDir.toAbsolutePath().normalize().toString());
        assertThat(newDir).as("storage directory should have been created").isDirectory();
    }

    @Test
    @DisplayName("notations() and schemaRegistries() default to empty maps when nothing is configured")
    void collectionAccessorsDefaultToEmpty() {
        final var ksmlConfig = new KSMLConfig();
        assertThat(ksmlConfig.notations()).as("notations should default to an empty map").isEmpty();
        assertThat(ksmlConfig.schemaRegistries()).as("schemaRegistries should default to an empty map").isEmpty();
        assertThat(ksmlConfig.definitions()).as("definitions should default to an empty map").isEmpty();
    }

    @Test
    @DisplayName("notations() exposes the configured entries as an unmodifiable map")
    void notationsAreExposedReadOnly() {
        final var notations = new KSMLConfig.NotationMap();
        notations.add("myAvro", NotationConfig.builder().type(NotationConfig.NotationType.CONFLUENT_AVRO).build());
        final var ksmlConfig = new KSMLConfig();
        ksmlConfig.notations(notations);
        final var ksmlNotations = ksmlConfig.notations();
        assertThat(ksmlNotations).hasSize(1);
        assertThat(ksmlNotations.get("myAvro").type()).isEqualTo(NotationConfig.NotationType.CONFLUENT_AVRO);
        assertThatThrownBy(ksmlNotations::clear)
                .as("the accessor must return a read-only view")
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    @DisplayName("schemaRegistries() exposes the configured entries as an unmodifiable map")
    void schemaRegistriesAreExposedReadOnly() {
        final var registries = new KSMLConfig.SchemaRegistryMap();
        registries.add("primary", SchemaRegistryConfig.builder().build());
        final var ksmlConfig = new KSMLConfig();
        ksmlConfig.schemaRegistries(registries);
        final var ksmlSchemaRegistries = ksmlConfig.schemaRegistries();
        assertThat(ksmlSchemaRegistries).hasSize(1).containsKey("primary");
        assertThatThrownBy(ksmlSchemaRegistries::clear)
                .as("the accessor must return a read-only view")
                .isInstanceOf(UnsupportedOperationException.class);
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
        assertThat(result).hasSize(1);
        assertThat(result.get("inlineNamespace")).isSameAs(inline);
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
        assertThat(loaded).as("the definition file should have been read").isNotNull();
        // The YAML content is parsed into a JsonNode tree we can inspect.
        assertThat(loaded.path("streams").has("some_stream")).as("the parsed definition should contain the stream").isTrue();
    }

    @Test
    @DisplayName("definitions() skips entries whose value is null")
    void definitionsSkipNullEntries() {
        final var defs = new KSMLConfig.KsmlDefinitionMap();
        defs.add("nullEntry", null);
        final var ksmlConfig = new KSMLConfig();
        ksmlConfig.definitions(defs);

        assertThat(ksmlConfig.definitions()).as("null definition entries must be skipped").isEmpty();
    }

    @Test
    @DisplayName("A directory accessor throws when the configured path is a regular file")
    void directoryAccessorRejectsNonDirectory(@TempDir Path tempDir) throws Exception {
        final var file = tempDir.resolve("not-a-dir.txt");
        Files.writeString(file, "content");
        final var ksmlConfig = new KSMLConfig();
        ksmlConfig.schemaDirectory(file.toString());

        assertThatThrownBy(ksmlConfig::schemaDirectory)
                .as("a path that is a regular file is not a valid directory")
                .isInstanceOf(ConfigException.class);
    }

    @Test
    @DisplayName("definitions() throws when a referenced file does not exist")
    void definitionsThrowOnMissingFile() {
        final var defs = new KSMLConfig.KsmlDefinitionMap();
        defs.add("missing", new KsmlFilePath("does-not-exist.yaml"));
        final var ksmlConfig = new KSMLConfig();
        ksmlConfig.configDirectory("src/test/resources");
        ksmlConfig.definitions(defs);

        assertThatThrownBy(ksmlConfig::definitions)
                .as("a missing definition file should fail fast")
                .isInstanceOf(ConfigException.class);
    }

    @Test
    @DisplayName("errorHandlingConfig() returns the configured instance when present")
    void errorHandlingConfigReturnsExistingInstance() {
        final var ksmlConfig = new KSMLConfig(); // field is initialized with a default instance
        final var existing = ksmlConfig.errorHandlingConfig();

        assertThat(existing).isNotNull();
        // A second call returns the same default instance (the non-null branch).
        assertThat(ksmlConfig.errorHandlingConfig()).isSameAs(existing);
    }

    @Test
    @DisplayName("storageDirectory creation fails with a ConfigException when the parent is a file")
    void storageDirectoryCreationFailsWhenParentIsAFile(@TempDir Path tempDir) throws Exception {
        final var blocker = tempDir.resolve("blocker");
        Files.writeString(blocker, "i am a file"); // a regular file where a directory parent is expected
        final var ksmlConfig = new KSMLConfig();
        ksmlConfig.storageDirectory(blocker.resolve("child").toString());
        ksmlConfig.createStorageDirectory(true);

        assertThatThrownBy(ksmlConfig::storageDirectory)
                .as("creating a directory under a regular file should fail")
                .isInstanceOf(ConfigException.class);
    }

    @Test
    @DisplayName("definitions() skips a referenced file that cannot be parsed")
    void definitionsSkipUnparseableFile(@TempDir Path tempDir) throws Exception {
        final var badFile = tempDir.resolve("broken.yaml");
        Files.writeString(badFile, "foo: [1, 2, 3"); // unterminated flow sequence -> parse error
        final var defs = new KSMLConfig.KsmlDefinitionMap();
        defs.add("broken", new KsmlFilePath("broken.yaml"));
        final var ksmlConfig = new KSMLConfig();
        ksmlConfig.configDirectory(tempDir.toString());
        ksmlConfig.definitions(defs);

        // The IOException while reading is caught and logged, so the entry is simply absent.
        assertThat(ksmlConfig.definitions()).as("an unparseable definition file is skipped").isEmpty();
    }

    @Test
    @DisplayName("pythonContextConfig() falls back to a default when none is configured")
    void pythonContextConfigDefaults() {
        final var ksmlConfig = new KSMLConfig();
        assertThat(ksmlConfig.pythonContextConfig()).as("a default python context config should be provided").isNotNull();

        final var explicit = PythonContextConfig.builder().build();
        ksmlConfig.pythonContextConfig(explicit);
        assertThat(ksmlConfig.pythonContextConfig()).as("an explicit config should be returned as-is").isSameAs(explicit);
    }

    @Test
    @DisplayName("errorHandlingConfig() and applicationServerConfig() recover from a null field")
    void configAccessorsRecreateNullFields() {
        final var ksmlConfig = new KSMLConfig();
        ksmlConfig.errorHandlingConfig(null);
        ksmlConfig.applicationServerConfig(null);

        assertThat(ksmlConfig.errorHandlingConfig()).as("a fresh error handling config should be returned").isNotNull();
        final var appServer = ksmlConfig.applicationServerConfig();
        assertThat(appServer).as("a fresh application server config should be returned").isNotNull();
        assertThat(appServer.enabled()).as("the recreated application server should be disabled by default").isFalse();
    }
}
