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


import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.apache.kafka.streams.StreamsConfig;

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
        @JsonProperty(value = StreamsConfig.APPLICATION_ID_CONFIG, required = true)
        @JsonPropertyDescription("An identifier for the stream processing application. Must be unique within the Kafka cluster. It is used as 1) the default client-id prefix, 2) the group-id for membership management, 3) the changelog topic prefix.")
        private String applicationId;

        @Nonnull
        @JsonProperty(value = StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, required = true)
        @JsonPropertyDescription("""
                A list of host/port pairs used to establish the initial connection to the Kafka cluster. 
                Clients use this list to bootstrap and discover the full set of Kafka brokers.
                While the order of servers in the list does not matter, we recommend including more than one server to ensure resilience if any servers are down. 
                This list does not need to contain the entire set of brokers, as Kafka clients automatically manage and update connections to the cluster efficiently.
                This list must be in the form 'host1:port1,host2:port2,...' """)
        private String bootstrapServers;

        @Override
        public String put(final String property, final String value) {
            if (StreamsConfig.APPLICATION_ID_CONFIG.equals(property)) {
                this.applicationId = value;
            }
            if (StreamsConfig.BOOTSTRAP_SERVERS_CONFIG.equals(property)) {
                this.bootstrapServers = value;
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
