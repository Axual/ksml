package io.axual.ksml.client.resolving;

/*-
 * ========================LICENSE_START=================================
 * Extended Kafka clients for KSML
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

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import io.axual.ksml.client.exception.InvalidPatternException;
import lombok.extern.slf4j.Slf4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

@Slf4j
class PatternResolverTest {
    private static final String TEST_FIELD_A = "a";
    private static final String TEST_FIELD_B = "b";
    private static final String TEST_FIELD_C = "c";

    private static final String TEST_PATTERN_1 = "{a}-{b}-{c}";
    private static final String TEST_PATTERN_2 = "{b}-{c}-{a}";

    private static final String TEST_VALUE_A = "11";
    private static final String TEST_VALUE_B = "22";
    private static final String TEST_VALUE_C = "33";

    private static final String RESOLVED_TEST_PATTERN_1 = "11-22-33";
    private static final String RESOLVED_TEST_PATTERN_2 = "22-33-11";

    private static final Map<String, String> UNRESOLVED_CONTEXT = Map.of(
            TEST_FIELD_A, TEST_VALUE_A,
            TEST_FIELD_B, TEST_VALUE_B
    );

    record TestSet(String pattern, String testFieldName, String testFieldValue, String resolved,
                   Map<String, String> context) {
    }

    static Stream<Arguments> testSets() {
        return Stream.of(
                Arguments.arguments(new TestSet(TEST_PATTERN_1, TEST_FIELD_C, TEST_VALUE_C, RESOLVED_TEST_PATTERN_1, UNRESOLVED_CONTEXT)),
                Arguments.arguments(new TestSet(TEST_PATTERN_2, TEST_FIELD_C, TEST_VALUE_C, RESOLVED_TEST_PATTERN_2, UNRESOLVED_CONTEXT))
        );
    }

    @ParameterizedTest
    @DisplayName("Resolve topic name from context")
    @MethodSource("testSets")
    void resolveTopicNameFromContext(TestSet testSet) {
        final var converter = new PatternResolver(testSet.pattern(), testSet.testFieldName(), testSet.context());
        assertThat(converter.resolve(testSet.testFieldValue()))
                .as("Resolve name from context")
                .isEqualTo(testSet.resolved());
    }

    @ParameterizedTest
    @DisplayName("Unresolve name to context")
    @MethodSource("testSets")
    void unresolveNameToContext(TestSet testSet) {
        final Map<String, String> expectedContext = new HashMap<>(testSet.context());
        expectedContext.put(testSet.testFieldName(), testSet.testFieldValue());

        final var converter = new PatternResolver(testSet.pattern(), testSet.testFieldName(), testSet.context());
        assertThat(converter.unresolveContext(testSet.resolved()))
                .as("Unresolve name to context")
                .containsExactlyInAnyOrderEntriesOf(expectedContext);
    }

    @ParameterizedTest
    @DisplayName("Unresolve name and extract single field")
    @MethodSource("testSets")
    void unresolveNameToSingleField(TestSet testSet) {
        final var converter = new PatternResolver(testSet.pattern(), testSet.testFieldName(), testSet.context());
        assertThat(converter.unresolve(testSet.resolved()))
                .as("Unresolve name to context")
                .isEqualTo(testSet.testFieldValue());
    }

    @ParameterizedTest
    @DisplayName("Invalid patterns throws exceptions")
    @NullSource
    @EmptySource
    @ValueSource(strings = {"   ", "{c", "c}", "c", "{c}-{b", "{c}-b}", "{c}-{b}-{a", "{c}-{b}-a}", "{c}-{d}", "{d}"})
    void invalidPattern(String pattern) {
        assertThatCode(() -> new PatternResolver(pattern, TEST_FIELD_C, UNRESOLVED_CONTEXT))
                .isInstanceOf(InvalidPatternException.class)
                .asInstanceOf(InstanceOfAssertFactories.type(InvalidPatternException.class))
                .returns(pattern, InvalidPatternException::pattern)
        ;
    }

    @ParameterizedTest
    @DisplayName("Invalid defaultField throws exceptions")
    @NullSource
    @EmptySource
    @ValueSource(strings = {"   ", "{a", "a}"})
    void invalidDefaultField(String defaultFieldName) {
        assertThatCode(() -> new PatternResolver(TEST_PATTERN_1, defaultFieldName, UNRESOLVED_CONTEXT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("defaultFieldName")
        ;
    }
}
