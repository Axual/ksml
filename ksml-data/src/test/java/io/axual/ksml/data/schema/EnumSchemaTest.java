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

import io.axual.ksml.data.type.Symbol;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class EnumSchemaTest {

    @Test
    @DisplayName("EnumSchema: STRING is assignable and superset of symbols is required")
    void enumAssignabilityRules() {
        EnumSchema colors = new EnumSchema("ns", "Color", "doc", List.of(new Symbol("RED"), new Symbol("GREEN")));
        EnumSchema redOnly = new EnumSchema("ns", "Color", "doc", List.of(new Symbol("RED")));
        EnumSchema blueOnly = new EnumSchema("ns", "Color", "doc", List.of(new Symbol("BLUE")));

        // Strings are assignable to enum
        assertThat(colors.isAssignableFrom(DataSchema.STRING_SCHEMA)).isTrue();

        // Superset rule: colors accepts from redOnly
        assertThat(colors.isAssignableFrom(redOnly)).isTrue();
        // But redOnly does not accept from colors
        assertThat(redOnly.isAssignableFrom(colors)).isFalse();

        // No overlap: should be false
        assertThat(colors.isAssignableFrom(blueOnly)).isFalse();
    }
}
