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

import java.util.HashMap;
import java.util.Map;

import io.axual.ksml.client.exception.InvalidPatternException;

import static org.assertj.core.api.Assertions.assertThatCode;

class TransactionalIdPatternResolverTest {

    public static final String EXPECTED_DEFAULT_PLACEHOLDER = "transactional.id";
    public static final String PATTERN_WITH_DEFAULT_PLACEHOLDER = "{tenant}-{instance}-{environment}-{transactional.id}";
    public static final String TENANT = "kmsl";
    public static final String INSTANCE = "testing";
    public static final String ENVIRONMENT = "test";

    public static final String TRANSACTIONAL_ID = "example-transactional-id";
    public static final String RESOLVED_TOPIC_NAME = "%s-%s-%s-%s".formatted(TENANT, INSTANCE, ENVIRONMENT, TRANSACTIONAL_ID);

    public static final Map<String, String> BASE_CONFIG = Map.of(
            "tenant", TENANT,
            "instance", INSTANCE,
            "environment", ENVIRONMENT);


    @Test
    @DisplayName("Resolve transactional id")
    void resolveValidPatternAndData() {
        var resolver = new TransactionalIdPatternResolver(PATTERN_WITH_DEFAULT_PLACEHOLDER, BASE_CONFIG);
        var expectedUnresolvedContext = new HashMap<>(BASE_CONFIG);
        expectedUnresolvedContext.put(EXPECTED_DEFAULT_PLACEHOLDER, TRANSACTIONAL_ID);

        var softly = new SoftAssertions();
        softly.assertThat(resolver.resolve(TRANSACTIONAL_ID))
                .as("Transactional id is properly resolved")
                .isEqualTo(RESOLVED_TOPIC_NAME);
        softly.assertThat(resolver.unresolve(RESOLVED_TOPIC_NAME))
                .as("Transactional id is properly unresolved")
                .isEqualTo(TRANSACTIONAL_ID);
        softly.assertThat(resolver.unresolveContext(RESOLVED_TOPIC_NAME))
                .as("Transactional id is properly unresolved to context")
                .containsExactlyInAnyOrderEntriesOf(expectedUnresolvedContext);


        softly.assertAll();
    }

    @ParameterizedTest
    @DisplayName("Incorrect, empty or null patterns causes exception")
    @NullSource
    @EmptySource
    @ValueSource(strings = {"   ", "nothing-here", "{topic"})
    void nullPatternException(String pattern) {
        assertThatCode(() -> new TransactionalIdPatternResolver(pattern, BASE_CONFIG))
                .as("Instantiation should cause exception")
                .isInstanceOf(InvalidPatternException.class);
    }

}
