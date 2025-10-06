package io.axual.ksml.data.schema;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FixedSchemaTest {

    @Test
    @DisplayName("Constructor rejects negative size and accepts zero")
    void constructorValidation() {
        assertThatThrownBy(() -> new FixedSchema("ns", "Fixed", "doc", -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Size of FIXED type can not be smaller than zero. Found -1");

        final var zero = new FixedSchema("ns", "Zero", "doc", 0);
        assertThat(zero.size()).isZero();
        assertThat(zero).hasToString("ns.Zero");
    }

    @Test
    @DisplayName("Assignability based on size and type")
    void checkAssignableFromBehavior() {
        final var eight = new FixedSchema("ns", "F8", "", 8);
        final var four = new FixedSchema("ns", "F4", "", 4);

        // same or smaller size: assignable
        assertThat(eight.checkAssignableFrom(eight).isOK()).isTrue();
        assertThat(eight.checkAssignableFrom(four).isOK()).isTrue();

        // larger size: not assignable
        assertThat(four.checkAssignableFrom(eight).isOK()).isFalse();

        // different type (eg string schema) not assignable
        assertThat(eight.checkAssignableFrom(DataSchema.STRING_SCHEMA).isOK()).isFalse();

        // null not assignable
        assertThat(eight.checkAssignableFrom(null).isOK()).isFalse();
    }
}
