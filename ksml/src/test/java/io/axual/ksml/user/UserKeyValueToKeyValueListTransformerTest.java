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
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.TupleType;
import io.axual.ksml.exception.ExecutionException;
import io.axual.ksml.type.UserType;
import org.junit.jupiter.api.Test;

import static io.axual.ksml.user.UserTestSupport.functionReturning;
import static io.axual.ksml.user.UserTestSupport.tags;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class UserKeyValueToKeyValueListTransformerTest {

    private static final UserType LIST_OF_TUPLES = new UserType(new ListType(new TupleType(DataType.UNKNOWN, DataType.UNKNOWN)));

    private UserKeyValueToKeyValueListTransformer transformer(io.axual.ksml.data.object.DataObject result) {
        return new UserKeyValueToKeyValueListTransformer(functionReturning(LIST_OF_TUPLES, 2, result), tags());
    }

    @Test
    void convertsListOfTuplesIntoKeyValues() {
        final var list = new DataList(DataType.UNKNOWN);
        list.add(new DataTuple(new DataString("k1"), new DataString("v1")));
        list.add(new DataTuple(new DataString("k2"), new DataString("v2")));

        assertThat(transformer(list).apply("key", "value")).hasSize(2);
    }

    @Test
    void returnsEmptyListWhenFunctionReturnsNull() {
        assertThat(transformer(null).apply("key", "value")).isEmpty();
    }

    @Test
    void failsWhenFunctionDoesNotReturnList() {
        final var transformer = transformer(new DataString("notAList"));
        assertThatThrownBy(() -> transformer.apply("key", "value"))
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("list");
    }
}
