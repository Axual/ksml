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
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.ParameterDefinition;
import io.axual.ksml.definition.PythonSource;
import io.axual.ksml.definition.ValueJoinerDefinition;
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

class UserValueJoinerTest {

    private UserValueJoiner joiner(DataString result) {
        return new UserValueJoiner(functionReturning(UNKNOWN, 3, result), tags());
    }

    @Test
    @DisplayName("joining two values returns the function result")
    void joinsTwoValues() {
        final var result = new DataString("joined");
        assertThat(joiner(result).apply("v1", "v2")).isEqualTo(result);
    }

    @Test
    @DisplayName("joining two values with a key returns the function result")
    void joinsWithKey() {
        final var result = new DataString("joined");
        assertThat(joiner(result).apply("key", "v1", "v2")).isEqualTo(result);
    }

    @Test
    @DisplayName("a valueJoiner function has an optional mappedKey parameter after value2")
    void definitionHasOptionalMappedKeyParameter() {
        final var params = valueJoinerParameters();

        assertThat(params).extracting(ParameterDefinition::name)
                .containsExactly("key", "value1", "value2", "mappedKey");
        assertThat(params[3].isOptional()).isTrue();
    }

    @Test
    @DisplayName("in a globalTable join the function gets the stream key, both values and the mapped key")
    void passesMappedKeyToFunction() {
        final var function = new RecordingFunction(UNKNOWN, valueJoinerParameters(), new DataString("joined"));

        new UserValueJoiner(function, tags()).apply("order1", "customer1", "orderValue", "customerValue");

        assertThat(function.lastArguments).containsExactly(
                new DataString("order1"), new DataString("orderValue"), new DataString("customerValue"), new DataString("customer1"));
    }

    @Test
    @DisplayName("in other joins the function does not get a mapped key")
    void doesNotPassMappedKeyInOtherJoins() {
        final var function = new RecordingFunction(UNKNOWN, valueJoinerParameters(), new DataString("joined"));

        new UserValueJoiner(function, tags()).apply("key", "v1", "v2");

        assertThat(function.lastArguments).containsExactly(new DataString("key"), new DataString("v1"), new DataString("v2"));
    }

    @Test
    @DisplayName("a function without a mappedKey parameter gets the stream key and both values")
    void doesNotPassMappedKeyToFunctionWithoutMappedKeyParameter() {
        final var function = new RecordingFunction(UNKNOWN, params(3), new DataString("joined"));

        new UserValueJoiner(function, tags()).apply("order1", "customer1", "orderValue", "customerValue");

        assertThat(function.lastArguments).containsExactly(
                new DataString("order1"), new DataString("orderValue"), new DataString("customerValue"));
    }

    @Test
    @DisplayName("a function with too few parameters is rejected with a clear message")
    void rejectsTooFewParameters() {
        final var function = functionReturning(UNKNOWN, 2, new DataString("joined"));

        assertThatThrownBy(() -> new UserValueJoiner(function, tags()))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("Function needs at least 3 parameters, but has 2");
    }

    @Test
    @DisplayName("a function with too many required parameters is rejected with a clear message")
    void rejectsTooManyRequiredParameters() {
        final var function = functionReturning(UNKNOWN, 4, new DataString("joined"));

        assertThatThrownBy(() -> new UserValueJoiner(function, tags()))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("Function can have at most 3 required parameters, but has 4");
    }

    @Test
    @DisplayName("a function whose fourth parameter has another name does not get the mapped key")
    void doesNotPassMappedKeyToOtherFourthParameter() {
        final var params = new ParameterDefinition[]{params(3)[0], params(3)[1], params(3)[2],
                new ParameterDefinition("other", DataType.UNKNOWN, true, null)};
        final var function = new RecordingFunction(UNKNOWN, params, new DataString("joined"));

        new UserValueJoiner(function, tags()).apply("order1", "customer1", "orderValue", "customerValue");

        assertThat(function.lastArguments).containsExactly(
                new DataString("order1"), new DataString("orderValue"), new DataString("customerValue"));
    }

    @Test
    @DisplayName("custom parameters come before the optional mappedKey parameter")
    void customParametersComeBeforeMappedKey() {
        assertThat(valueJoinerParameters(customPrefix())).extracting(ParameterDefinition::name)
                .containsExactly("key", "value1", "value2", "prefix", "mappedKey");
    }

    @Test
    @DisplayName("a function with a custom parameter gets None for it and the mapped key last")
    void passesNoneForCustomParameterAndMappedKeyLast() {
        final var function = new RecordingFunction(UNKNOWN, valueJoinerParameters(customPrefix()), new DataString("joined"));

        new UserValueJoiner(function, tags()).apply("order1", "customer1", "orderValue", "customerValue");

        assertThat(function.lastArguments).containsExactly(
                new DataString("order1"), new DataString("orderValue"), new DataString("customerValue"),
                DataNull.INSTANCE, new DataString("customer1"));
    }

    private static ParameterDefinition customPrefix() {
        return new ParameterDefinition("prefix", DataString.DATATYPE, true, "D");
    }

    private static ParameterDefinition[] valueJoinerParameters(ParameterDefinition... customParameters) {
        final var function = FunctionDefinition.as("generic", "fn", List.of(customParameters),
                PythonSource.of((String) null, (String) null, (String) null), UNKNOWN, List.of());
        return new ValueJoinerDefinition(function).parameters();
    }
}
