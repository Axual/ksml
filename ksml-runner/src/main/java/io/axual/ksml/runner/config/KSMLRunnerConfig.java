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
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;
import lombok.extern.slf4j.Slf4j;

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

    public Map<String, String> getKafkaConfig() {
        var newConfig = new HashMap<>(kafkaConfig.kafkaConfig());
        newConfig.put("application.id", kafkaConfig.applicationId());
        return newConfig;
    }

    public String getApplicationId() {
        return kafkaConfig.applicationId();
    }

    @Data
    public static class KafkaConfig {
        @JsonProperty("app.id")
        @JsonAlias({"applicationId", "application.id"})
        public String applicationId;

        @JsonIgnore
        private Map<String, String> kafkaConfig = new HashMap<>();

        // Capture all other fields that Jackson do not match other members
        @JsonAnyGetter
        public Map<String, String> kafkaConfig() {
            return kafkaConfig;
        }

        @JsonAnySetter
        public void setOtherField(String name, String value) {
            kafkaConfig.put(name, value);
        }
    }
}
