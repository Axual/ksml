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

import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataTuple;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.definition.ParameterDefinition;
import io.axual.ksml.exception.ExecutionException;
import io.axual.ksml.exception.TopologyException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.axual.ksml.user.UserTestSupport.UNKNOWN;
import static io.axual.ksml.user.UserTestSupport.params;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class UserFunctionTest {

    private static UserFunction function(ParameterDefinition[] parameters, List<String> storeNames) {
        return new UserFunction("ns", "fn", parameters, UNKNOWN, storeNames);
    }

    @Test
    @DisplayName("constructing a function with a null namespace throws TopologyException")
    void rejectsNullNamespace() {
        final var parameters = params(0);
        assertThatThrownBy(() -> new UserFunction(null, "fn", parameters, UNKNOWN, List.of()))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("namespace");
    }

    @Test
    @DisplayName("constructing a function with a null name throws TopologyException")
    void rejectsNullName() {
        final var parameters = params(0);
        assertThatThrownBy(() -> new UserFunction("ns", null, parameters, UNKNOWN, List.of()))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("name");
    }

    @Test
    @DisplayName("a fixed parameter declared after an optional one throws TopologyException")
    void rejectsFixedParameterAfterOptionalParameter() {
        final var parameters = new ParameterDefinition[]{
                new ParameterDefinition("optional", DataType.UNKNOWN, true, null),
                new ParameterDefinition("fixed", DataType.UNKNOWN, false, null)
        };
        assertThatThrownBy(() -> function(parameters, List.of()))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("fixed parameters should be listed first");
    }

    @Test
    @DisplayName("fixedParameterCount counts only the non-optional parameters")
    void countsFixedParameters() {
        final var parameters = new ParameterDefinition[]{
                new ParameterDefinition("fixed", DataType.UNKNOWN, false, null),
                new ParameterDefinition("optional", DataType.UNKNOWN, true, null)
        };
        assertThat(function(parameters, List.of()).fixedParameterCount).isEqualTo(1);
    }

    @Test
    @DisplayName("toString renders the function signature and its store names")
    void toStringRendersSignatureAndStores() {
        final var function = function(params(1), List.of("store1", "store2"));
        assertThat(function).asString()
                .contains("ns.fn")
                .contains("==>")
                .contains("store1", "store2");
    }

    @Test
    @DisplayName("calling the base function directly throws ExecutionException requiring an override")
    void baseCallCannotBeInvokedDirectly() {
        final var function = function(params(0), List.of());
        assertThatThrownBy(function::call)
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("Override this class");
    }

    @Test
    @DisplayName("convertToKeyValue turns a two-element DataList into a key/value pair")
    void convertsDataListToKeyValue() {
        final var function = function(params(0), List.of());
        final var list = new DataList(DataType.UNKNOWN);
        list.add(new DataString("k"));
        list.add(new DataString("v"));

        final var keyValue = function.convertToKeyValue(list, DataType.UNKNOWN, DataType.UNKNOWN);

        assertThat(keyValue.key).isEqualTo(new DataString("k"));
        assertThat(keyValue.value).isEqualTo(new DataString("v"));
    }

    @Test
    @DisplayName("convertToKeyValue turns a DataTuple into a key/value pair")
    void convertsDataTupleToKeyValue() {
        final var function = function(params(0), List.of());
        final var tuple = new DataTuple(new DataString("k"), new DataString("v"));

        final var keyValue = function.convertToKeyValue(tuple, DataType.UNKNOWN, DataType.UNKNOWN);

        assertThat(keyValue.key).isEqualTo(new DataString("k"));
        assertThat(keyValue.value).isEqualTo(new DataString("v"));
    }

    @Test
    @DisplayName("convertToKeyValue throws TopologyException for a value that is not a pair")
    void convertToKeyValueFailsForNonPair() {
        final var function = function(params(0), List.of());
        assertThatThrownBy(() -> function.convertToKeyValue(new DataString("x"), DataType.UNKNOWN, DataType.UNKNOWN))
                .isInstanceOf(TopologyException.class);
    }
}
