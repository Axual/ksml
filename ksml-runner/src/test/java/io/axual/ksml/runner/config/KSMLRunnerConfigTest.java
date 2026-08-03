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

import tools.jackson.databind.DatabindException;
import tools.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KSMLRunnerConfigTest {

    // Use the exact mapper the runner uses to read its config, so this test validates real behavior.
    private final ObjectMapper objectMapper = RunnerConfigMapper.INSTANCE;

    @Test
    @DisplayName("complete config should load without exceptions")
    void shouldLoadWithoutExceptions() {
        final var yaml = getClass().getClassLoader().getResourceAsStream("ksml-runner-config.yaml");
        final var ksmlRunnerConfig = objectMapper.readValue(yaml, KSMLRunnerConfig.class);

        assertNotNull(ksmlRunnerConfig.getKsmlConfig());
        final var expectedKafkaConfig = new HashMap<String, String>();
        expectedKafkaConfig.put("bootstrap.servers","broker:9093");
        expectedKafkaConfig.put("application.id","io.ksml.example.processor");
        expectedKafkaConfig.put("schema.registry.url","http://schema_registry:8081");
        expectedKafkaConfig.put("acks","all");
        expectedKafkaConfig.put("axual.topic.pattern","{tenant}-{instance}-{environment}-{topic}");
        expectedKafkaConfig.put("axual.group.id.pattern","{tenant}-{instance}-{environment}-{group.id}");
        expectedKafkaConfig.put("axual.transactional.id.pattern","{tenant}-{instance}-{environment}-{transactional.id}");
        assertThat(ksmlRunnerConfig.getKafkaConfigMap())
                .isNotNull()
                .containsExactlyInAnyOrderEntriesOf(expectedKafkaConfig);
    }

    @Test
    @DisplayName("Missing pythonContext yields default PythonContextConfig")
    void missingPythonContextDefaults() throws Exception {
        var yaml = """
            configDirectory: /tmp/config
            schemaDirectory: /tmp/schema
            storageDirectory: /tmp/storage
            definitions:
              foo: {}
            """;
        var cfg = objectMapper.readValue(yaml, KSMLConfig.class);

        var pyCfg = cfg.pythonContextConfig();
        // all flags should be default false
        assertFalse(pyCfg.allowHostFileAccess());
        assertFalse(pyCfg.allowHostSocketAccess());
        assertFalse(pyCfg.allowNativeAccess());
        assertFalse(pyCfg.allowCreateProcess());
        assertFalse(pyCfg.allowCreateThread());
        assertFalse(pyCfg.inheritEnvironmentVariables());
    }

    @Test
    @DisplayName("Explicit pythonContext in YAML is picked up")
    void explicitPythonContext() throws Exception {
        var yaml = """
            configDirectory: /tmp/config
            schemaDirectory: /tmp/schema
            storageDirectory: /tmp/storage
            definitions:
              foo: {}
            pythonContext:
              allowHostFileAccess: true
              allowHostSocketAccess: false
              allowNativeAccess: true
              allowCreateProcess: false
              allowCreateThread: true
              inheritEnvironmentVariables: true
            """;
        var cfg = objectMapper.readValue(yaml, KSMLConfig.class);

        var pyCfg = cfg.pythonContextConfig();
        assertTrue(pyCfg.allowHostFileAccess(),     "should pick up allowHostFileAccess=true");
        assertFalse(pyCfg.allowHostSocketAccess(),  "should pick up allowHostSocketAccess=false");
        assertTrue(pyCfg.allowNativeAccess(),       "should pick up allowNativeAccess=true");
        assertFalse(pyCfg.allowCreateProcess(),     "should pick up allowCreateProcess=false");
        assertTrue(pyCfg.allowCreateThread(),       "should pick up allowCreateThread=true");
        assertTrue(pyCfg.inheritEnvironmentVariables(), "should pick up inheritEnvironmentVariables=true");
    }

    @Test
    @DisplayName("An unknown key in the runner config fails instead of being silently ignored")
    void unknownKeyFails() {
        final var yaml = """
            ksml:
              configDirectory: /tmp/config
              schemaRegsitry: oops
            kafka:
              application.id: test
            """;

        assertThatThrownBy(() -> objectMapper.readValue(yaml, KSMLRunnerConfig.class))
                .isInstanceOf(DatabindException.class)
                .hasMessageContaining("schemaRegsitry");
    }
}
