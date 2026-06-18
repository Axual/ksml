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
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.axual.ksml.runner.exception.ConfigException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
        ksmlConfig.configDirectory();
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
}
