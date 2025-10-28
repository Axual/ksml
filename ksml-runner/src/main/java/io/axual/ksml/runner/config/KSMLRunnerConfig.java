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


import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.axual.ksml.client.resolving.ResolvingClientConfig;
import io.axual.ksml.runner.config.internal.StringMap;
import jakarta.annotation.Nonnull;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@JsonIgnoreProperties(ignoreUnknown = false)
@NoArgsConstructor
public class KSMLRunnerConfig {
    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    @JsonProperty(value = "ksml", required = true)
    @Nonnull
    private KSMLConfig ksmlConfig;

    public void setKsmlConfig(KSMLConfig ksmlConfig) {
        this.ksmlConfig = ksmlConfig;
    }

    @Nonnull
    public KSMLConfig getKsmlConfig() {
        return ksmlConfig;
    }

    @JsonProperty(value = "kafka", required = true)
    @Nonnull
    private KafkaConfig kafkaConfig;

    public String getApplicationId() {
        return kafkaConfig.applicationId();
    }


    public void setKafkaConfig(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    public KafkaConfig getKafkaConfig() {
        return this.kafkaConfig;
    }

    public Map<String, String> getKafkaConfigMap() {
        return kafkaConfig.getEffectiveConfig();
    }

    @JsonClassDescription("Contains the Kafka Streams configuration options, like bootstrap servers, application ids, etc")
    @Data
    public static class KafkaConfig extends StringMap {
        @JsonCreator
        public KafkaConfig() {
            super();
        }

        @Nonnull
        @JsonProperty(value = "application.id", required = true)
        @JsonAlias({"applicationId", "app.id"})
        private String applicationId;

        @Override
        public String put(final String property, final String value) {
            if ("application.id".equals(property)) {
                this.applicationId = value;
            }
            return super.put(property, value);
        }

        @JsonIgnore
        public Map<String, String> getEffectiveConfig() {
            final var result = new HashMap<>(this);
            if (ResolvingClientConfig.configRequiresResolving(result)) {
                ResolvingClientConfig.replaceDeprecatedConfigKeys(result);
            }
            return Collections.unmodifiableMap(result);

        }
    }
}
