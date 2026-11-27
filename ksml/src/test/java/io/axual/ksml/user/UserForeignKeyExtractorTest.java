package io.axual.ksml.user;

/*-
 * ========================LICENSE_START=================================
 * KSML
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

import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.definition.ForeignKeyExtractorDefinition;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.ParameterDefinition;
import io.axual.ksml.definition.PythonSource;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.user.UserTestSupport.RecordingFunction;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.axual.ksml.user.UserTestSupport.UNKNOWN;
import static io.axual.ksml.user.UserTestSupport.functionReturning;
import static io.axual.ksml.user.UserTestSupport.params;
import static io.axual.ksml.user.UserTestSupport.tags;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class UserForeignKeyExtractorTest {

    @Test
    @DisplayName("apply returns the foreign key produced by the function for the value")
    void extractsForeignKey() {
        final var result = new DataString("foreignKey");
        final var extractor = new UserForeignKeyExtractor(functionReturning(UNKNOWN, 1, result), tags());

        assertThat(extractor.apply("key", "value")).isEqualTo(result);
    }

    @Test
    @DisplayName("a foreignKeyExtractor function has an optional key parameter after value")
    void definitionHasOptionalKeyParameter() {
        final var params = foreignKeyExtractorParameters();

        assertThat(params).extracting(ParameterDefinition::name).containsExactly("value", "key");
        assertThat(params[1].isOptional()).isTrue();
    }

    @Test
    @DisplayName("a function with a key parameter gets the value and the key")
    void passesKeyToFunction() {
        final var function = new RecordingFunction(UNKNOWN, foreignKeyExtractorParameters(), new DataString("c1"));

        new UserForeignKeyExtractor(function, tags()).apply("c1-o1", "orderValue");

        assertThat(function.lastArguments).containsExactly(new DataString("orderValue"), new DataString("c1-o1"));
    }

    @Test
    @DisplayName("a function without a key parameter gets only the value")
    void doesNotPassKeyToFunctionWithoutKeyParameter() {
        final var function = new RecordingFunction(UNKNOWN, params(1), new DataString("c1"));

        new UserForeignKeyExtractor(function, tags()).apply("c1-o1", "orderValue");

        assertThat(function.lastArguments).containsExactly(new DataString("orderValue"));
    }

    @Test
    @DisplayName("custom parameters come before the optional key parameter")
    void customParametersComeBeforeKey() {
        assertThat(foreignKeyExtractorParameters(customSeparator())).extracting(ParameterDefinition::name)
                .containsExactly("value", "separator", "key");
    }

    @Test
    @DisplayName("a function with a custom parameter gets None for it and the key last")
    void passesNoneForCustomParameterAndKeyLast() {
        final var function = new RecordingFunction(UNKNOWN, foreignKeyExtractorParameters(customSeparator()), new DataString("c1"));

        new UserForeignKeyExtractor(function, tags()).apply("c1-o1", "orderValue");

        assertThat(function.lastArguments).containsExactly(new DataString("orderValue"), DataNull.INSTANCE, new DataString("c1-o1"));
    }

    @Test
    @DisplayName("a custom parameter called key is replaced by the built-in one and gets the record key")
    void customKeyParameterGetsRecordKey() {
        final var customKey = new ParameterDefinition("key", DataString.DATATYPE, true, "x");
        final var params = foreignKeyExtractorParameters(customKey);
        final var function = new RecordingFunction(UNKNOWN, params, new DataString("c1"));

        new UserForeignKeyExtractor(function, tags()).apply("c1-o1", "orderValue");

        assertThat(params).extracting(ParameterDefinition::name).containsExactly("value", "key");
        assertThat(params[1].defaultValue()).isNull();
        assertThat(function.lastArguments).containsExactly(new DataString("orderValue"), new DataString("c1-o1"));
    }

    @Test
    @DisplayName("a function with too many required parameters is rejected with a clear message")
    void rejectsTooManyRequiredParameters() {
        final var function = functionReturning(UNKNOWN, 2, new DataString("c1"));

        assertThatThrownBy(() -> new UserForeignKeyExtractor(function, tags()))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("Function can have at most 1 required parameters, but has 2");
    }

    private static ParameterDefinition customSeparator() {
        return new ParameterDefinition("separator", DataString.DATATYPE, true, "-");
    }

    private static ParameterDefinition[] foreignKeyExtractorParameters(ParameterDefinition... customParameters) {
        final var function = FunctionDefinition.as("generic", "ns", "fn", List.of(customParameters),
                PythonSource.of((String) null, (String) null, (String) null), UNKNOWN, null, List.of());
        return new ForeignKeyExtractorDefinition(function).parameters();
    }
}
