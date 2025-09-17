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

class MapSchemaTest {

    @Test
    @DisplayName("Constructor rejects null value schema via @NonNull")
    void constructorNullValueSchemaThrows() {
        assertThatThrownBy(() -> new MapSchema(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("Value schema getter returns configured schema")
    void valueSchemaGetter() {
        var map = new MapSchema(DataSchema.STRING_SCHEMA);
        assertThat(map.valueSchema()).isEqualTo(DataSchema.STRING_SCHEMA);
    }

    @Test
    @DisplayName("Assignability is based on value schema assignability")
    void assignabilityPropagatesFromValueSchema() {
        final var mapOfInt = new MapSchema(DataSchema.INTEGER_SCHEMA);
        final var mapOfLong = new MapSchema(DataSchema.LONG_SCHEMA);
        final var mapOfString = new MapSchema(DataSchema.STRING_SCHEMA);
        final var mapOfFloat = new MapSchema(DataSchema.FLOAT_SCHEMA);
        final var mapOfDouble = new MapSchema(DataSchema.DOUBLE_SCHEMA);

        // integer group: integer accepts from long
        assertThat(mapOfInt.isAssignableFrom(mapOfInt)).isTrue();
        assertThat(mapOfInt.isAssignableFrom(mapOfLong)).isTrue();
        assertThat(mapOfInt.isAssignableFrom(mapOfString)).isFalse();

        // floating group: float/double accept from each other
        assertThat(mapOfFloat.isAssignableFrom(mapOfDouble)).isTrue();
        assertThat(mapOfDouble.isAssignableFrom(mapOfFloat)).isTrue();

        // Non-map should not be assignable
        assertThat(mapOfInt.isAssignableFrom(DataSchema.STRING_SCHEMA)).isFalse();
        // Null not assignable
        assertThat(mapOfInt.isAssignableFrom(null)).isFalse();
    }
}
