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

import io.axual.ksml.client.exception.InvalidPatternException;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatCode;

class GroupPatternResolverTest {

    public static final String PATTERN_WITH_DEFAULT_PLACEHOLDER = "{tenant}-{instance}-{environment}-{group.id}";
    public static final String PATTERN_WITH_ALIAS_PLACEHOLDER = "{tenant}-{instance}-{environment}-{group}";
    public static final String TENANT = "kmsl";
    public static final String INSTANCE = "testing";
    public static final String ENVIRONMENT = "test";

    public static final String GROUP_ID = "some.testing-group";
    public static final String RESOLVED_GROUP_ID = "%s-%s-%s-%s".formatted(TENANT, INSTANCE, ENVIRONMENT, GROUP_ID);

    public static final Map<String, String> BASE_CONFIG = Map.of(
            "tenant", TENANT,
            "instance", INSTANCE,
            "environment", ENVIRONMENT);

    @ParameterizedTest
    @DisplayName("Resolve groups")
    @ValueSource(strings = {PATTERN_WITH_DEFAULT_PLACEHOLDER, PATTERN_WITH_ALIAS_PLACEHOLDER})
    void resolveValidPatternAndData(String pattern) {
        var resolver = new GroupPatternResolver(pattern, BASE_CONFIG);

        var softly = new SoftAssertions();
        softly.assertThat(resolver.resolve(GROUP_ID))
                .as("Group name is properly resolved")
                .isEqualTo(RESOLVED_GROUP_ID);
        softly.assertThat(resolver.unresolve(RESOLVED_GROUP_ID))
                .as("Group name is properly unresolved")
                .isEqualTo(GROUP_ID);

        softly.assertAll();
    }

    @ParameterizedTest
    @DisplayName("Incorrect, empty or null patterns causes exception")
    @NullSource
    @EmptySource
    @ValueSource(strings = {"   ", "nothing-here", "{group.id"})
    void nullPatternException(String pattern) {
        assertThatCode(() -> new GroupPatternResolver(pattern, BASE_CONFIG))
                .as("Instantiation should cause exception")
                .isInstanceOf(InvalidPatternException.class);
    }

}
