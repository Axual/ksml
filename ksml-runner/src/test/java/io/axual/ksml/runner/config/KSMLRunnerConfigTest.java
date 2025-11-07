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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KSMLRunnerConfigTest {

    private ObjectMapper objectMapper;
    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    @BeforeEach
    void setup() {
        objectMapper = new ObjectMapper(new YAMLFactory());
    }

    @Test
    @DisplayName("complete config should load without exceptions")
    void shouldLoadWithoutExceptions() throws IOException {
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
        var cfg = mapper.readValue(yaml, KSMLConfig.class);

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
        var cfg = mapper.readValue(yaml, KSMLConfig.class);

        var pyCfg = cfg.pythonContextConfig();
        assertTrue(pyCfg.allowHostFileAccess(),     "should pick up allowHostFileAccess=true");
        assertFalse(pyCfg.allowHostSocketAccess(),  "should pick up allowHostSocketAccess=false");
        assertTrue(pyCfg.allowNativeAccess(),       "should pick up allowNativeAccess=true");
        assertFalse(pyCfg.allowCreateProcess(),     "should pick up allowCreateProcess=false");
        assertTrue(pyCfg.allowCreateThread(),       "should pick up allowCreateThread=true");
        assertTrue(pyCfg.inheritEnvironmentVariables(), "should pick up inheritEnvironmentVariables=true");
    }
}
