package io.axual.ksml.data.compare;

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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EqualityTest {

    @Test
    @DisplayName("equal() is the OK state: equal, not-not-equal, no message")
    void equalState() {
        final var equality = Equality.equal();

        assertThat(equality.isEqual()).isTrue();
        assertThat(equality.isNotEqual()).isFalse();
        assertThat(equality.message()).isNull();
        assertThat(equality.cause()).isNull();
    }

    @Test
    @DisplayName("notEqual(message) carries the message and no cause")
    void notEqualWithMessage() {
        final var equality = Equality.notEqual("they differ");

        assertThat(equality.isEqual()).isFalse();
        assertThat(equality.isNotEqual()).isTrue();
        assertThat(equality.message()).isEqualTo("they differ");
        assertThat(equality.cause()).isNull();
    }

    @Test
    @DisplayName("notEqual stores a non-equal cause but drops an OK cause")
    void notEqualCauseHandling() {
        final var realCause = Equality.notEqual("inner reason");

        assertThat(Equality.notEqual("outer", realCause).cause()).isSameAs(realCause);
        // An OK cause is meaningless for an inequality, so it is dropped.
        assertThat(Equality.notEqual("outer", Equality.equal()).cause()).isNull();
    }

    @Test
    @DisplayName("notEqual(null) is rejected")
    void notEqualRejectsNullMessage() {
        assertThatThrownBy(() -> Equality.notEqual(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("toString renders the message and chains causes with 'caused by:'")
    void toStringRendersChain() {
        final var chained = Equality.notEqual("outer", Equality.notEqual("inner"));

        assertThat(chained.toString()).contains("outer");
        assertThat(chained.toString()).contains("caused by:").contains("inner");
    }
}
