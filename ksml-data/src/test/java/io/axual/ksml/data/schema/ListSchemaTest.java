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

class ListSchemaTest {

    @Test
    @DisplayName("hasName/name default and explicit name behavior")
    void nameBehavior() {
        final var unnamed = new ListSchema(DataSchema.STRING_SCHEMA);
        assertThat(unnamed.hasName()).isFalse();
        assertThat(unnamed.name()).isEqualTo("AnonymousListSchema");

        final var named = new ListSchema("MyList", DataSchema.STRING_SCHEMA);
        assertThat(named.hasName()).isTrue();
        assertThat(named.name()).isEqualTo("MyList");
    }

    @Test
    @DisplayName("toString shows type and element schema")
    void toStringFormat() {
        final var listOfInt = new ListSchema(DataSchema.INTEGER_SCHEMA);
        assertThat(listOfInt).hasToString("list of integer");
    }

    @Test
    @DisplayName("Assignability is based on element schema assignability")
    void assignabilityPropagatesFromElementSchema() {
        final var listOfInt = new ListSchema(DataSchema.INTEGER_SCHEMA);
        final var listOfLong = new ListSchema(DataSchema.LONG_SCHEMA);
        final var listOfString = new ListSchema(DataSchema.STRING_SCHEMA);
        final var listOfFloat = new ListSchema(DataSchema.FLOAT_SCHEMA);
        final var listOfDouble = new ListSchema(DataSchema.DOUBLE_SCHEMA);

        // Integers: integer accepts from long
        assertThat(listOfInt.isAssignableFrom(listOfInt).isAssignable()).isTrue();
        assertThat(listOfInt.isAssignableFrom(listOfLong).isAssignable()).isTrue();
        assertThat(listOfInt.isAssignableFrom(listOfString).isAssignable()).isFalse();

        // Floating: float and double accept from each other
        assertThat(listOfFloat.isAssignableFrom(listOfDouble).isAssignable()).isTrue();
        assertThat(listOfDouble.isAssignableFrom(listOfFloat).isAssignable()).isTrue();

        // Non-list should not be assignable
        assertThat(listOfInt.isAssignableFrom(DataSchema.STRING_SCHEMA).isAssignable()).isFalse();
        // Null not assignable
        assertThat(listOfInt.isAssignableFrom(null).isAssignable()).isFalse();
    }
}
