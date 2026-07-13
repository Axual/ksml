package io.axual.ksml.data.value;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
 * %%
 * Copyright (C) 2021 - 2026 Axual B.V.
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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TupleTest {

    @Test
    @DisplayName("A tuple exposes its ordered elements")
    void exposesElements() {
        final var tuple = new Tuple<>("a", "b", "c");

        assertThat(tuple.elements()).containsExactly("a", "b", "c");
    }

    @Test
    @DisplayName("toString renders the elements comma-separated in parentheses")
    void rendersToString() {
        assertThat(new Tuple<>("a", "b")).hasToString("(a, b)");
    }

    @Test
    @DisplayName("Tuples with equal elements are equal and share a hash code")
    void valueEquality() {
        final var tuple = new Tuple<>("a", "b");
        assertThat(tuple)
                .isEqualTo(new Tuple<>("a", "b"))
                .hasSameHashCodeAs(new Tuple<>("a", "b"))
                .isNotEqualTo(new Tuple<>("a", "c"));
    }
}
