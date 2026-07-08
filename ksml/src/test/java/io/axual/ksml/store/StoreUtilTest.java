package io.axual.ksml.store;

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
import io.axual.ksml.definition.KeyValueStateStoreDefinition;
import io.axual.ksml.definition.SessionStateStoreDefinition;
import io.axual.ksml.definition.StateStoreDefinition;
import io.axual.ksml.definition.WindowStateStoreDefinition;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.type.UserType;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class StoreUtilTest {

    private static final UserType UNKNOWN = UserType.UNKNOWN;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void registerNotation() {
        final var notation = mock(Notation.class);
        final Serde<Object> serde = mock(Serde.class);
        when(serde.serializer()).thenReturn(mock(Serializer.class));
        when(serde.deserializer()).thenReturn(mock(Deserializer.class));
        when(notation.serde(any(), anyBoolean())).thenReturn(serde);
        ExecutionContext.INSTANCE.notationLibrary().register(UserType.DEFAULT_NOTATION, notation);
    }

    private static KeyValueStateStoreDefinition keyValueStore(boolean persistent, boolean timestamped, boolean versioned, boolean caching, boolean logging) {
        return new KeyValueStateStoreDefinition("store", persistent, timestamped, versioned,
                Duration.ofSeconds(900), Duration.ofSeconds(60), UNKNOWN, UNKNOWN, caching, logging);
    }

    private static WindowStateStoreDefinition windowStore(boolean persistent, boolean timestamped, boolean retainDuplicates, Duration retention, Duration windowSize) {
        return new WindowStateStoreDefinition("store", persistent, timestamped, retention, windowSize, retainDuplicates, UNKNOWN, UNKNOWN, false, false);
    }

    private static SessionStateStoreDefinition sessionStore(boolean persistent, boolean caching, boolean logging) {
        return new SessionStateStoreDefinition("store", persistent, false, Duration.ofSeconds(60), UNKNOWN, UNKNOWN, caching, logging);
    }

    // --- getStoreBuilder: key/value --------------------------------------------------------------

    @Test
    void buildsInMemoryKeyValueStore() {
        assertThat(StoreUtil.getStoreBuilder(keyValueStore(false, false, false, true, true))).isNotNull();
    }

    @Test
    void buildsInMemoryTimestampedKeyValueStore() {
        assertThat(StoreUtil.getStoreBuilder(keyValueStore(false, true, false, false, false))).isNotNull();
    }

    @Test
    void buildsPersistentKeyValueStore() {
        assertThat(StoreUtil.getStoreBuilder(keyValueStore(true, false, false, true, false))).isNotNull();
    }

    @Test
    void buildsPersistentTimestampedKeyValueStore() {
        assertThat(StoreUtil.getStoreBuilder(keyValueStore(true, true, false, false, true))).isNotNull();
    }

    @Test
    void buildsPersistentVersionedKeyValueStore() {
        // Versioned stores do not support caching, so caching must be disabled.
        assertThat(StoreUtil.getStoreBuilder(keyValueStore(true, false, true, false, true))).isNotNull();
    }

    // --- getStoreBuilder: session & window -------------------------------------------------------

    @Test
    void buildsSessionStore() {
        assertThat(StoreUtil.getStoreBuilder(sessionStore(true, true, true))).isNotNull();
        assertThat(StoreUtil.getStoreBuilder(sessionStore(false, false, false))).isNotNull();
    }

    @Test
    void buildsWindowStore() {
        assertThat(StoreUtil.getStoreBuilder(windowStore(true, false, false, Duration.ofSeconds(60), Duration.ofSeconds(10)))).isNotNull();
        assertThat(StoreUtil.getStoreBuilder(windowStore(true, true, false, Duration.ofSeconds(60), Duration.ofSeconds(10)))).isNotNull();
        assertThat(StoreUtil.getStoreBuilder(windowStore(false, false, false, Duration.ofSeconds(60), Duration.ofSeconds(10)))).isNotNull();
    }

    @Test
    void rejectsUnknownStoreType() {
        final var unknown = mock(StateStoreDefinition.class);
        assertThatThrownBy(() -> StoreUtil.getStoreBuilder(unknown))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown store type");
    }

    // --- materialize -----------------------------------------------------------------------------

    @Test
    void materializesKeyValueStore() {
        assertThat(StoreUtil.materialize(keyValueStore(false, false, false, false, false)).materialized()).isNotNull();
    }

    @Test
    void materializesSessionStore() {
        assertThat(StoreUtil.materialize(sessionStore(true, false, false)).materialized()).isNotNull();
    }

    @Test
    void materializesWindowStore() {
        assertThat(StoreUtil.materialize(windowStore(true, false, false, Duration.ofSeconds(60), Duration.ofSeconds(10))).materialized()).isNotNull();
    }

    // --- validatedWindowStore --------------------------------------------------------------------

    private static JoinWindows joinWindows() {
        return JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(1));
    }

    @Test
    void validatesMatchingWindowStore() {
        final var store = windowStore(false, false, true, Duration.ofSeconds(2), Duration.ofSeconds(2));
        assertThat(StoreUtil.validatedWindowStore(store, joinWindows())).isNotNull();
    }

    @Test
    void rejectsWindowStoreWithoutRetainDuplicates() {
        final var store = windowStore(false, false, false, Duration.ofSeconds(2), Duration.ofSeconds(2));
        final var windows = joinWindows();
        assertThatThrownBy(() -> StoreUtil.validatedWindowStore(store, windows))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("retainDuplicates");
    }

    @Test
    void rejectsWindowStoreWithWrongWindowSize() {
        // windowSize (3s) differs from the join window size (2s); retention stays >= windowSize.
        final var store = windowStore(false, false, true, Duration.ofSeconds(4), Duration.ofSeconds(3));
        final var windows = joinWindows();
        assertThatThrownBy(() -> StoreUtil.validatedWindowStore(store, windows))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("windowSize");
    }

    @Test
    void rejectsWindowStoreWithWrongRetention() {
        final var store = windowStore(false, false, true, Duration.ofSeconds(10), Duration.ofSeconds(2));
        final var windows = joinWindows();
        assertThatThrownBy(() -> StoreUtil.validatedWindowStore(store, windows))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("retention");
    }
}
