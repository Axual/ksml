package io.axual.ksml.execution;

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

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.notation.Notation;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

class NotationLibraryTest {

    private final NotationLibrary library = new NotationLibrary();

    @Test
    @DisplayName("register then look up returns the notation and is case-insensitive on the name")
    void registerAndLookupIsCaseInsensitive() {
        final var notation = mock(Notation.class);
        library.register("Avro", notation);

        assertThat(library.exists("avro")).isTrue();
        assertThat(library.get("AVRO")).isSameAs(notation);
        assertThat(library.getIfExists("aVrO")).isSameAs(notation);
    }

    @Test
    @DisplayName("get on an unknown notation throws DataException naming the notation")
    void getUnknownThrows() {
        assertThatThrownBy(() -> library.get("missing"))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("missing");
    }

    @Test
    @DisplayName("getIfExists returns null for an unknown notation")
    void getIfExistsUnknownReturnsNull() {
        assertThat(library.getIfExists("missing")).isNull();
    }

    @Test
    @DisplayName("a null notation name resolves via the null key: getIfExists returns null and get throws")
    void nullNameLookup() {
        assertThat(library.getIfExists(null)).isNull();
        assertThatThrownBy(() -> library.get(null))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("null");
    }

    @Test
    @DisplayName("unregister removes a registered notation (case-insensitive)")
    void unregisterRemovesRegisteredNotation() {
        library.register("Avro", mock(Notation.class));

        library.unregister("aVrO");

        assertThat(library.exists("avro")).isFalse();
        assertThat(library.getIfExists("avro")).isNull();
    }

    @Test
    @DisplayName("unregister with a null name removes the null-keyed entry without throwing")
    void unregisterNullName() {
        library.register(null, mock(Notation.class));
        assertThat(library.exists(null)).isTrue();

        library.unregister(null);

        assertThat(library.exists(null)).isFalse();
    }

    @Test
    @DisplayName("unregister on an absent notation is a no-op")
    void unregisterAbsentIsNoop() {
        library.unregister("nothing");

        assertThat(library.exists("nothing")).isFalse();
    }
}
