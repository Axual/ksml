package io.axual.ksml.definition;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
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

class PythonSourceTest {

    @Test
    @DisplayName("equal contents are equal regardless of null-vs-empty normalization")
    void equalsTreatsNullAndEmptyGlobalCodeAndCodeAsEqual() {
        final var first = new PythonSource(null, null, new String[]{"expr"});
        final var second = new PythonSource(new String[]{}, new String[]{}, new String[]{"expr"});

        assertThat(first).isEqualTo(second);
    }

    @Test
    @DisplayName("differing code content is not equal")
    void equalsDetectsDifferingContent() {
        final var first = new PythonSource(null, new String[]{"a"}, null);
        final var second = new PythonSource(null, new String[]{"b"}, null);

        assertThat(first).isNotEqualTo(second);
    }

    @Test
    @DisplayName("a null expression equals another null expression")
    void equalsTreatsNullExpressionAsEqual() {
        final var first = new PythonSource(null, null, null);
        final var second = new PythonSource(null, null, null);

        assertThat(first).isEqualTo(second);
    }

    @Test
    @DisplayName("a null expression is not equal to an empty expression")
    void equalsDistinguishesNullFromEmptyExpression() {
        final var first = new PythonSource(null, null, null);
        final var second = new PythonSource(null, null, new String[]{});

        assertThat(first).isNotEqualTo(second);
    }

    @Test
    @DisplayName("equal instances share the same hash code")
    void equalInstancesShareHashCode() {
        final var first = new PythonSource(null, new String[]{"a", "b"}, new String[]{"expr"});
        final var second = new PythonSource(new String[]{}, new String[]{"a", "b"}, new String[]{"expr"});

        assertThat(first).hasSameHashCodeAs(second);
    }

    @Test
    @DisplayName("toString summarizes line counts instead of dumping array contents")
    void toStringSummarizesLineCounts() {
        final var source = new PythonSource(null, new String[]{"line1"}, new String[]{"line1", "line2"});

        assertThat(source).asString()
                .isEqualTo("PythonSource[globalCode=none, code=1 line, expression=2 lines]");
    }

    @Test
    @DisplayName("toString reports a null expression explicitly")
    void toStringReportsNullExpression() {
        final var source = new PythonSource(null, null, null);

        assertThat(source).asString()
                .isEqualTo("PythonSource[globalCode=none, code=none, expression=null]");
    }
}
