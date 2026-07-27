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

import io.axual.ksml.data.notation.Notation;
import io.axual.ksml.data.schema.NamedSchema;
import io.axual.ksml.exception.ExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SchemaLibraryTest {

    @Mock
    private Notation notation;
    @Mock
    private NamedSchema remoteSchema;

    private SchemaLibrary library;

    @BeforeEach
    void setUp() {
        library = new SchemaLibrary();
    }

    @Test
    @DisplayName("getSchema by name returns null for an unknown schema when null is allowed")
    void getSchemaByNameReturnsNullWhenUnknownAndNullAllowed() {
        assertThat(library.getSchema("unknown", true)).isNull();
    }

    @Test
    @DisplayName("getSchema by name throws for an unknown schema when null is not allowed")
    void getSchemaByNameThrowsWhenUnknownAndNullNotAllowed() {
        assertThatThrownBy(() -> library.getSchema("unknown", false))
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("Unknown schema");
    }

    @Test
    @DisplayName("getSchema by notation returns null when the notation has no schema parser")
    void getSchemaByNotationReturnsNullWhenNoParser() {
        when(notation.name()).thenReturn("avro");
        when(notation.schemaParser()).thenReturn(null);
        assertThat(library.getSchema(notation, "unknown", true)).isNull();
    }

    @Test
    @DisplayName("getOrFetchRemoteSchema caches the result so the remote is fetched only once")
    void getOrFetchRemoteSchemaCachesResult() {
        when(notation.name()).thenReturn("avro");
        when(notation.fetchRemoteSchema("topic", false)).thenReturn(remoteSchema);

        assertThat(library.getOrFetchRemoteSchema(notation, "topic", false)).isSameAs(remoteSchema);
        // second call is served from cache
        assertThat(library.getOrFetchRemoteSchema(notation, "topic", false)).isSameAs(remoteSchema);

        // the remote schema must be fetched only once; the second lookup is served from cache
        verify(notation, times(1)).fetchRemoteSchema("topic", false);
    }
}
