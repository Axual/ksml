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
import org.junit.jupiter.api.Test;

import static io.axual.ksml.user.UserTestSupport.UNKNOWN;
import static io.axual.ksml.user.UserTestSupport.functionReturning;
import static io.axual.ksml.user.UserTestSupport.tags;
import static org.assertj.core.api.Assertions.assertThat;

class UserKeyTransformerTest {

    private UserKeyTransformer transformer(DataString result) {
        return new UserKeyTransformer(functionReturning(UNKNOWN, 2, result), tags());
    }

    @Test
    void transformsKeyWithoutStores() {
        final var result = new DataString("newKey");
        assertThat(transformer(result).apply("key", "value")).isEqualTo(result);
    }

    @Test
    void transformsKeyWithStores() {
        final var result = new DataString("newKey");
        assertThat(transformer(result).apply(null, "key", "value")).isEqualTo(result);
    }
}
