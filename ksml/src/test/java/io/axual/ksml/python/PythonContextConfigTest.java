package io.axual.ksml.python;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
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
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class PythonContextConfigTest {

    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    @Test
    @DisplayName("Builder defaults all flags to false")
    void defaultBuilderFlags() {
        var cfg = PythonContextConfig.builder().build();

        SoftAssertions.assertSoftly(softly -> {
            softly.assertThat(cfg.allowHostFileAccess())
                    .as("allowHostFileAccess should default to false")
                    .isFalse();
            softly.assertThat(cfg.allowHostSocketAccess())
                    .as("allowHostSocketAccess should default to false")
                    .isFalse();
            softly.assertThat(cfg.allowNativeAccess())
                    .as("allowNativeAccess should default to false")
                    .isFalse();
            softly.assertThat(cfg.allowCreateProcess())
                    .as("allowCreateProcess should default to false")
                    .isFalse();
            softly.assertThat(cfg.allowCreateThread())
                    .as("allowCreateThread should default to false")
                    .isFalse();
            softly.assertThat(cfg.inheritEnvironmentVariables())
                    .as("inheritEnvironmentVariables should default to false")
                    .isFalse();
        });
    }

    @Test
    @DisplayName("YAML mapping populates all flags correctly")
    void yamlMappingFlags() throws Exception {
        var yaml = """
            allowHostFileAccess: true
            allowHostSocketAccess: true
            allowNativeAccess: true
            allowCreateProcess: true
            allowCreateThread: true
            inheritEnvironmentVariables: true
            """;
        var cfg = mapper.readValue(yaml, PythonContextConfig.class);

        SoftAssertions.assertSoftly(softly -> {
            softly.assertThat(cfg.allowHostFileAccess())
                    .as("allowHostFileAccess should be true")
                    .isTrue();
            softly.assertThat(cfg.allowHostSocketAccess())
                    .as("allowHostSocketAccess should be true")
                    .isTrue();
            softly.assertThat(cfg.allowNativeAccess())
                    .as("allowNativeAccess should be true")
                    .isTrue();
            softly.assertThat(cfg.allowCreateProcess())
                    .as("allowCreateProcess should be true")
                    .isTrue();
            softly.assertThat(cfg.allowCreateThread())
                    .as("allowCreateThread should be true")
                    .isTrue();
            softly.assertThat(cfg.inheritEnvironmentVariables())
                    .as("inheritEnvironmentVariables should be true")
                    .isTrue();
        });
    }
}
