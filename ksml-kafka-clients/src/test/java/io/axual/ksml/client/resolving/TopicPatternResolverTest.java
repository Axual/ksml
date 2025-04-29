package io.axual.ksml.client.resolving;

/*-
 * ========================LICENSE_START=================================
 * KSML Kafka clients
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

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import io.axual.ksml.client.exception.InvalidPatternException;

import static org.assertj.core.api.Assertions.assertThatCode;

class TopicPatternResolverTest {

    public static final String TOPIC_FIELD_NAME = "topic";
    public static final String PATTERN_WITH_DEFAULT_PLACEHOLDER = "{tenant}-{instance}-{environment}-{topic}";
    public static final String TENANT = "kmsl";
    public static final String INSTANCE = "testing";
    public static final String ENVIRONMENT = "test";

    public static final String TOPIC_NAME = "example-topic";
    public static final String INTERNAL_TOPIC_NAME = "_internal-example-topic";
    public static final String RESOLVED_TOPIC_NAME = "%s-%s-%s-%s".formatted(TENANT, INSTANCE, ENVIRONMENT, TOPIC_NAME);

    public static final Map<String, String> BASE_CONFIG = Map.of(
            "tenant", TENANT,
            "instance", INSTANCE,
            "environment", ENVIRONMENT);


    @Test
    @DisplayName("Resolve topics")
    void resolveValidPatternAndData() {
        var resolver = new TopicPatternResolver(PATTERN_WITH_DEFAULT_PLACEHOLDER, BASE_CONFIG);

        var softly = new SoftAssertions();
        softly.assertThat(resolver.resolve(TOPIC_NAME))
                .as("Topic name is properly resolved")
                .isEqualTo(RESOLVED_TOPIC_NAME);
        softly.assertThat(resolver.unresolve(RESOLVED_TOPIC_NAME))
                .as("Topic name is properly unresolved")
                .isEqualTo(TOPIC_NAME);

        // Check internal topic resolving
        softly.assertThat(resolver.resolve(INTERNAL_TOPIC_NAME))
                .as("Internal topic name is returned as is")
                .isEqualTo(INTERNAL_TOPIC_NAME);
        softly.assertThat(resolver.unresolve(INTERNAL_TOPIC_NAME))
                .as("Internal topic name is properly unresolved")
                .isEqualTo(INTERNAL_TOPIC_NAME);

        var expectedContext = Map.of(TOPIC_FIELD_NAME, INTERNAL_TOPIC_NAME);
        softly.assertThat(resolver.unresolveContext(INTERNAL_TOPIC_NAME))
                .as("Internal topic name is properly unresolved to context")
                .containsExactlyEntriesOf(expectedContext);


        softly.assertAll();
    }

    @ParameterizedTest
    @DisplayName("Incorrect, empty or null patterns causes exception")
    @NullSource
    @EmptySource
    @ValueSource(strings = {"   ", "nothing-here", "{topic"})
    void nullPatternException(String pattern) {
        assertThatCode(() -> new TopicPatternResolver(pattern, BASE_CONFIG))
                .as("Instantiation should cause exception")
                .isInstanceOf(InvalidPatternException.class);
    }

}
