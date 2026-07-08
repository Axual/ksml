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

import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataTuple;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.TupleType;
import io.axual.ksml.type.UserType;
import org.junit.jupiter.api.Test;

import static io.axual.ksml.user.UserTestSupport.functionReturning;
import static io.axual.ksml.user.UserTestSupport.tags;
import static org.assertj.core.api.Assertions.assertThat;

class UserKeyValueTransformerTest {

    private static final UserType TUPLE = new UserType(new TupleType(DataType.UNKNOWN, DataType.UNKNOWN));

    @Test
    void transformsIntoKeyValue() {
        final var result = new DataTuple(new DataString("newKey"), new DataString("newValue"));
        final var transformer = new UserKeyValueTransformer(functionReturning(TUPLE, 2, result), tags());

        final var keyValue = transformer.apply("key", "value");

        assertThat(keyValue.key).isEqualTo(new DataString("newKey"));
        assertThat(keyValue.value).isEqualTo(new DataString("newValue"));
    }
}
