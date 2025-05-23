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

import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ResolvingClientConfigTest {
    private static final String TOPIC_PATTERN_CONFIG = "axual.topic.pattern";
    private static final String GROUP_ID_PATTERN_CONFIG = "axual.group.id.pattern";
    private static final String TRANSACTIONAL_ID_PATTERN_CONFIG = "axual.transactional.id.pattern";

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

}
