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

import io.axual.ksml.data.compare.EqualityFlags;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class EnumSchemaTest {

    @Test
    @DisplayName("EnumSchema: STRING is assignable and superset of symbols is required")
    void enumAssignabilityRules() {
        final var colors = new EnumSchema("ns", "Color", "doc", List.of(new EnumSchema.Symbol("RED"), new EnumSchema.Symbol("GREEN")));
        final var redOnly = new EnumSchema("ns", "Color", "doc", List.of(new EnumSchema.Symbol("RED")));
        final var blueOnly = new EnumSchema("ns", "Color", "doc", List.of(new EnumSchema.Symbol("BLUE")));

        // Strings are assignable to enum
        assertThat(colors.isAssignableFrom(DataSchema.STRING_SCHEMA).isAssignable()).isTrue();

        // Superset rule: colors accepts from redOnly
        assertThat(colors.isAssignableFrom(redOnly).isAssignable()).isTrue();
        // But redOnly does not accept from colors
        assertThat(redOnly.isAssignableFrom(colors).isAssignable()).isFalse();

        // No overlap: should be false
        assertThat(colors.isAssignableFrom(blueOnly).isAssignable()).isFalse();
    }

    @Test
    @DisplayName("Symbol exposes name/doc/tag; the name-only constructor defaults doc and tag")
    void symbolAccessors() {
        final var full = new EnumSchema.Symbol("RED", "the colour red", 3);
        assertThat(full.name()).isEqualTo("RED");
        assertThat(full.doc()).isEqualTo("the colour red");
        assertThat(full.tag()).isEqualTo(3);

        final var nameOnly = new EnumSchema.Symbol("GREEN");
        assertThat(nameOnly.name()).isEqualTo("GREEN");
        assertThat(nameOnly.doc()).isNull();
    }

    @Test
    @DisplayName("symbols() returns the configured symbols")
    void symbolsAccessor() {
        final var colors = new EnumSchema("ns", "Color", "doc", List.of(new EnumSchema.Symbol("RED"), new EnumSchema.Symbol("GREEN")));

        assertThat(colors.symbols()).hasSize(2);
        assertThat(colors.symbols().get(0).name()).isEqualTo("RED");
    }

    @Test
    @DisplayName("Deep equals treats enums with the same symbols as equal and differing symbols as not equal")
    void deepEquals() {
        final var colors = new EnumSchema("ns", "Color", "doc", List.of(new EnumSchema.Symbol("RED")));
        final var sameColors = new EnumSchema("ns", "Color", "doc", List.of(new EnumSchema.Symbol("RED")));
        final var otherColors = new EnumSchema("ns", "Color", "doc", List.of(new EnumSchema.Symbol("BLUE")));

        assertThat(colors.equals(sameColors, EqualityFlags.EMPTY).isEqual()).isTrue();
        assertThat(colors.equals(otherColors, EqualityFlags.EMPTY).isNotEqual()).isTrue();
        assertThat(colors.equals(DataSchema.STRING_SCHEMA, EqualityFlags.EMPTY).isNotEqual()).isTrue();
    }
}
