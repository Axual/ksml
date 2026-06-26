package io.axual.ksml.runner.config;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
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

import io.axual.ksml.client.resolving.ResolvingClientConfig;
import io.axual.ksml.runner.config.KSMLRunnerConfig.KafkaConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link KafkaConfig}, covering the {@code put} validation guards for the mandatory
 * application id and bootstrap servers, and the effective-config resolution.
 */
class KafkaConfigTest {

    @Test
    @DisplayName("Putting the application id and bootstrap servers updates the typed fields")
    void putUpdatesTypedFields() {
        final var config = new KafkaConfig();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092");

        assertThat(config.applicationId()).isEqualTo("my-app");
        assertThat(config.bootstrapServers()).isEqualTo("broker:9092");
        assertThat(config).containsEntry(StreamsConfig.APPLICATION_ID_CONFIG, "my-app");
    }

    @Test
    @DisplayName("An arbitrary config key is stored without affecting the typed fields")
    void putArbitraryKeyIsStored() {
        final var config = new KafkaConfig();

        config.put("acks", "all");

        assertThat(config).containsEntry("acks", "all");
        assertThat(config.applicationId()).isEmpty();
    }

    @Test
    @DisplayName("A blank application id or bootstrap servers value is rejected")
    void putRejectsBlankMandatoryValues() {
        final var config = new KafkaConfig();

        assertThatThrownBy(() -> config.put(StreamsConfig.APPLICATION_ID_CONFIG, " "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("applicationId");
        assertThatThrownBy(() -> config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bootstrapServers");
    }

    @Test
    @DisplayName("A null application id or bootstrap servers value is rejected")
    void putRejectsNullMandatoryValues() {
        final var config = new KafkaConfig();

        assertThatThrownBy(() -> config.put(StreamsConfig.APPLICATION_ID_CONFIG, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("applicationId");
        assertThatThrownBy(() -> config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bootstrapServers");
    }

    @Test
    @DisplayName("Effective config is an unmodifiable copy of the entries")
    void effectiveConfigIsUnmodifiableCopy() {
        final var config = new KafkaConfig();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092");

        final var effective = config.getEffectiveConfig();

        assertThat(effective)
                .containsEntry(StreamsConfig.APPLICATION_ID_CONFIG, "my-app")
                .containsEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092");
        assertThatThrownBy(() -> effective.put("x", "y")).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    @DisplayName("Deprecated resolving keys are replaced when the config requires resolving")
    void effectiveConfigReplacesDeprecatedResolvingKeys() {
        final var config = new KafkaConfig();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092");
        // The deprecated topic pattern key triggers the resolving-config replacement path.
        config.put(ResolvingClientConfig.COMPAT_TOPIC_PATTERN_CONFIG, "{topic}");

        final var effective = config.getEffectiveConfig();

        // After replacement the canonical resolving key is present.
        assertThat(effective).containsKey(ResolvingClientConfig.TOPIC_PATTERN_CONFIG);
    }
}
