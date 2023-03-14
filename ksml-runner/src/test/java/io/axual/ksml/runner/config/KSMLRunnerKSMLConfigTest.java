package io.axual.ksml.runner.config;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
 * %%
 * Copyright (C) 2021 - 2022 Axual B.V.
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.axual.ksml.runner.exception.KSMLRunnerConfigurationException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
        final var ksmlConfig = objectMapper.readValue(yaml, KSMLRunnerKSMLConfig.class);

        ksmlConfig.validate();
    }

    @Test
    @DisplayName("validate of incorrect config directory should throw exception")
    void shouldThrowOnWrongConfigdir() throws Exception {
        final var yaml = getClass().getClassLoader().getResourceAsStream("ksml-config-wrong-configdir.yaml");
        final var ksmlConfig = objectMapper.readValue(yaml, KSMLRunnerKSMLConfig.class);

        assertThrows(KSMLRunnerConfigurationException.class, ksmlConfig::validate, "should throw exception for wrong configdir");
    }

    @Test
    @DisplayName("if configdir is missing it should default to workdir")
    void shouldDefaultConfigToWorkdir() throws Exception {
        final var yaml = getClass().getClassLoader().getResourceAsStream("ksml-config-no-configdir.yaml");
        final var ksmlConfig = objectMapper.readValue(yaml, KSMLRunnerKSMLConfig.class);

        ksmlConfig.validate();

        assertEquals(".", ksmlConfig.getConfigurationDirectory(), "config dir should default to working dir");
    }
}
