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


import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import io.axual.ksml.client.resolving.ResolvingClientConfig;
import lombok.Builder;
import lombok.Data;
import lombok.Singular;
import lombok.extern.jackson.Jacksonized;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
@Builder
@Jacksonized
public class KSMLRunnerConfig {
    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    @JsonProperty("ksml")
    private KSMLConfig ksmlConfig;

    @JsonProperty("kafka")
    private KafkaConfig kafkaConfig;

    public String applicationId() {
        return kafkaConfig.applicationId();
    }

    public Map<String, String> kafkaConfig() {
        return kafkaConfig.config();
    }

    @Data
    static class KafkaConfig {
        private final String applicationId;

        private final Map<String, String> config;

        @Builder
        @Jacksonized
        private KafkaConfig(@JsonProperty("app.id")
                            @JsonAlias({"applicationId", "application.id"}) String applicationId, @JsonAnySetter @Singular() Map<String, String> kafkaConfigs) {
            this.applicationId = applicationId;
            final var result = new HashMap<>(kafkaConfigs);
            if (ResolvingClientConfig.configRequiresResolving(result)) {
                ResolvingClientConfig.replaceDeprecatedConfigKeys(result);
            }
            // Add the application id back to the map
            result.put("application.id", applicationId);
            this.config = Collections.unmodifiableMap(result);
        }
    }
}
