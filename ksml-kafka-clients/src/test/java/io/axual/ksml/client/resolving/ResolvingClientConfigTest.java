package io.axual.ksml.client.resolving;

/*-
 * ========================LICENSE_START=================================
 * KSML Kafka clients
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

import io.axual.ksml.client.exception.ClientException;
import org.apache.kafka.common.Configurable;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ResolvingClientConfigTest {
    private static final String TOPIC_PATTERN_CONFIG = "axual.topic.pattern";
    private static final String GROUP_ID_PATTERN_CONFIG = "axual.group.id.pattern";
    private static final String TRANSACTIONAL_ID_PATTERN_CONFIG = "axual.transactional.id.pattern";
    private static final String INSTANCE_CONFIG = "instance.config";

    private static final String EXTRA_PATTERN_FIELD = "instance";
    private static final String TOPIC_PATTERN = "{instance}-{topic}";
    private static final String GROUP_ID_PATTERN = "{instance}-{group.id}";
    private static final String TRANSACTIONAL_ID_PATTERN = "{instance}-{transactional.id}";

    private static final String EXPECTED_DEFAULT_TOPIC_PATTERN = "{topic}";
    private static final String EXPECTED_DEFAULT_GROUP_ID_PATTERN = "{group.id}";
    private static final String EXPECTED_DEFAULT_TRANSACTIONAL_ID_PATTERN = "{transactional.id}";

    @Test
    @DisplayName("Pattern Resolvers use provided patterns")
    void allResolverConfigsSet() {
        var configAll = Map.of(
                TOPIC_PATTERN_CONFIG, TOPIC_PATTERN,
                GROUP_ID_PATTERN_CONFIG, GROUP_ID_PATTERN,
                TRANSACTIONAL_ID_PATTERN_CONFIG, TRANSACTIONAL_ID_PATTERN,
                EXTRA_PATTERN_FIELD, ""
        );

        final var clientConfig = new ResolvingClientConfig(configAll);

        assertThat(clientConfig)
                .isNotNull();

        verifyPatterns(clientConfig, TOPIC_PATTERN, GROUP_ID_PATTERN, TRANSACTIONAL_ID_PATTERN);
    }

    @Test
    @DisplayName("Missing Pattern Resolver patterns use default patterns")
    void noResolverConfigsSet() {
        final var clientConfig = new ResolvingClientConfig(Map.of());

        assertThat(clientConfig)
                .isNotNull();

        verifyPatterns(clientConfig, EXPECTED_DEFAULT_TOPIC_PATTERN, EXPECTED_DEFAULT_GROUP_ID_PATTERN, EXPECTED_DEFAULT_TRANSACTIONAL_ID_PATTERN);
    }

    static void verifyPatterns(ResolvingClientConfig clientConfig, String expectedTopicPattern, String expectedGroupIdPattern, String expectedTransactionalIdPattern) {
        assertThat(clientConfig)
                .isNotNull();

        final var softly = new SoftAssertions();

        softly.assertThat(clientConfig.topicResolver())
                .as("Verify the topic resolver is a pattern resolver with the provided pattern")
                .isNotNull()
                .isInstanceOf(CachedPatternResolver.class)
                .asInstanceOf(InstanceOfAssertFactories.type(CachedPatternResolver.class))
                .returns(expectedTopicPattern, CachedPatternResolver::pattern);

        softly.assertThat(clientConfig.groupResolver())
                .as("Verify the group resolver is a pattern resolver with the provided pattern")
                .isNotNull()
                .isInstanceOf(CachedPatternResolver.class)
                .asInstanceOf(InstanceOfAssertFactories.type(CachedPatternResolver.class))
                .returns(expectedGroupIdPattern, CachedPatternResolver::pattern);

        softly.assertThat(clientConfig.transactionalIdResolver())
                .as("Verify the transactional id resolver is a pattern resolver with the provided pattern")
                .isNotNull()
                .isInstanceOf(CachedPatternResolver.class)
                .asInstanceOf(InstanceOfAssertFactories.type(CachedPatternResolver.class))
                .returns(expectedTransactionalIdPattern, CachedPatternResolver::pattern);

        softly.assertAll();
    }

    @Test
    @DisplayName("An already-instantiated value is returned as-is without being configured")
    void getConfiguredInstanceFromInstance() {
        final var instance = new ConfigurableMarker();
        final var config = new ResolvingClientConfig(Map.of(INSTANCE_CONFIG, instance));

        final var result = config.getConfiguredInstance(INSTANCE_CONFIG, Marker.class);

        assertThat(result).isSameAs(instance);
        assertThat(instance.configured).isFalse();
    }

    @Test
    @DisplayName("A class-name value is instantiated and configured")
    void getConfiguredInstanceFromClassName() {
        final var config = new ResolvingClientConfig(Map.of(INSTANCE_CONFIG, ConfigurableMarker.class.getName()));

        final var result = config.getConfiguredInstance(INSTANCE_CONFIG, Marker.class);

        assertThat(result)
                .isInstanceOf(ConfigurableMarker.class)
                .asInstanceOf(InstanceOfAssertFactories.type(ConfigurableMarker.class))
                .extracting(m -> m.configured)
                .isEqualTo(true);
    }

    @Test
    @DisplayName("A Class value is instantiated and configured")
    void getConfiguredInstanceFromClass() {
        final var config = new ResolvingClientConfig(Map.of(INSTANCE_CONFIG, ConfigurableMarker.class));

        final var result = config.getConfiguredInstance(INSTANCE_CONFIG, Marker.class);

        assertThat(result)
                .isInstanceOf(ConfigurableMarker.class)
                .asInstanceOf(InstanceOfAssertFactories.type(ConfigurableMarker.class))
                .extracting(m -> m.configured)
                .isEqualTo(true);
    }

    @Test
    @DisplayName("An incompatible value returns null when null is allowed")
    void getConfiguredInstanceAllowsNull() {
        final var config = new ResolvingClientConfig(Map.of(INSTANCE_CONFIG, 42));

        assertThat(config.getConfiguredInstance(INSTANCE_CONFIG, Marker.class, true)).isNull();
    }

    @Test
    @DisplayName("An incompatible value throws when null is not allowed")
    void getConfiguredInstanceThrowsOnIncompatibleValue() {
        final var config = new ResolvingClientConfig(Map.of(INSTANCE_CONFIG, 42));

        assertThatThrownBy(() -> config.getConfiguredInstance(INSTANCE_CONFIG, Marker.class))
                .isInstanceOf(ClientException.class)
                .hasMessageContaining("Marker");
    }

    @Test
    @DisplayName("configRequiresResolving detects both current and deprecated pattern keys")
    void configRequiresResolving() {
        assertThat(ResolvingClientConfig.configRequiresResolving(Map.of())).isFalse();
        assertThat(ResolvingClientConfig.configRequiresResolving(Map.of(TOPIC_PATTERN_CONFIG, TOPIC_PATTERN))).isTrue();
        assertThat(ResolvingClientConfig.configRequiresResolving(Map.of("topic.pattern", TOPIC_PATTERN))).isTrue();
    }

    @Test
    @DisplayName("Deprecated pattern keys are replaced by their current equivalents")
    void replaceDeprecatedConfigKeys() {
        final Map<String, String> config = new HashMap<>();
        config.put("topic.pattern", TOPIC_PATTERN);
        config.put("group.id.pattern", GROUP_ID_PATTERN);
        config.put("transactional.id.pattern", TRANSACTIONAL_ID_PATTERN);

        ResolvingClientConfig.replaceDeprecatedConfigKeys(config);

        assertThat(config)
                .doesNotContainKeys("topic.pattern", "group.id.pattern", "transactional.id.pattern")
                .containsEntry(TOPIC_PATTERN_CONFIG, TOPIC_PATTERN)
                .containsEntry(GROUP_ID_PATTERN_CONFIG, GROUP_ID_PATTERN)
                .containsEntry(TRANSACTIONAL_ID_PATTERN_CONFIG, TRANSACTIONAL_ID_PATTERN);
    }

    @Test
    @DisplayName("Deprecated keys do not overwrite already-present current keys")
    void replaceDeprecatedConfigKeysDoesNotOverwriteCurrentKey() {
        final Map<String, String> config = new HashMap<>();
        config.put("topic.pattern", "deprecated-topic");
        config.put(TOPIC_PATTERN_CONFIG, TOPIC_PATTERN);
        config.put("group.id.pattern", "deprecated-group");
        config.put(GROUP_ID_PATTERN_CONFIG, GROUP_ID_PATTERN);
        config.put("transactional.id.pattern", "deprecated-tx");
        config.put(TRANSACTIONAL_ID_PATTERN_CONFIG, TRANSACTIONAL_ID_PATTERN);

        ResolvingClientConfig.replaceDeprecatedConfigKeys(config);

        assertThat(config)
                .doesNotContainKeys("topic.pattern", "group.id.pattern", "transactional.id.pattern")
                .containsEntry(TOPIC_PATTERN_CONFIG, TOPIC_PATTERN)
                .containsEntry(GROUP_ID_PATTERN_CONFIG, GROUP_ID_PATTERN)
                .containsEntry(TRANSACTIONAL_ID_PATTERN_CONFIG, TRANSACTIONAL_ID_PATTERN);
    }

    public interface Marker {
    }

    public static class ConfigurableMarker implements Marker, Configurable {
        private boolean configured;

        public ConfigurableMarker() {
            // Public no-arg constructor required for reflective instantiation by Kafka's Utils.newInstance
        }

        @Override
        public void configure(Map<String, ?> configs) {
            configured = true;
        }
    }

}
