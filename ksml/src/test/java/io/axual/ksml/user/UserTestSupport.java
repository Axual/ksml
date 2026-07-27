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

import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.definition.ParameterDefinition;
import io.axual.ksml.metric.MetricTags;
import io.axual.ksml.store.StateStores;
import io.axual.ksml.type.UserType;

/**
 * Shared helpers for unit-testing the {@code User*} function wrappers without the GraalVM Python
 * runtime.
 *
 * <p>Each wrapper delegates to {@link UserFunction#call}, which the real (Python-backed) function
 * overrides. Here we provide a {@link UserFunction} whose {@code call} returns a canned
 * {@link DataObject}, so a wrapper's {@code apply}/{@code test}/{@code transform} logic — argument
 * conversion, result conversion and validation — runs on any JVM.
 */
final class UserTestSupport {

    static final UserType UNKNOWN = UserType.UNKNOWN;

    private UserTestSupport() {
    }

    static MetricTags tags() {
        return new MetricTags();
    }

    static ParameterDefinition[] params(int count) {
        final var result = new ParameterDefinition[count];
        for (var index = 0; index < count; index++) {
            result[index] = new ParameterDefinition("p" + index, DataType.UNKNOWN);
        }
        return result;
    }

    static UserFunction functionReturning(UserType resultType, int paramCount, DataObject result) {
        return functionReturning(resultType, params(paramCount), result);
    }

    static UserFunction functionReturning(UserType resultType, ParameterDefinition[] params, DataObject result) {
        return new UserFunction("ns", "fn", params, resultType, (String[]) null) {
            @Override
            public DataObject call(StateStores stores, DataObject... parameters) {
                return result;
            }
        };
    }
}
