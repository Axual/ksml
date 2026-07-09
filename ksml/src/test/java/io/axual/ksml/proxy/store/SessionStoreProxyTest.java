package io.axual.ksml.proxy.store;

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

import io.axual.ksml.data.mapper.DataObjectFlattener;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataString;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class SessionStoreProxyTest {

    @Mock
    private SessionStore<Object, Object> delegate;
    @Mock
    private KeyValueIterator<Windowed<Object>, Object> iterator;

    private SessionStoreProxy proxy() {
        return new SessionStoreProxy(delegate);
    }

    private static Stream<Arguments> singleKeyOperations() {
        return Stream.of(
                Arguments.of("fetch", (Function<SessionStoreProxy, Object>) p -> p.fetch("key")),
                Arguments.of("backwardFetch", (Function<SessionStoreProxy, Object>) p -> p.backwardFetch("key")),
                Arguments.of("findSessions", (Function<SessionStoreProxy, Object>) p -> p.findSessions("key", 0L, 10L)),
                Arguments.of("backwardFindSessions", (Function<SessionStoreProxy, Object>) p -> p.backwardFindSessions("key", 0L, 10L)));
    }

    private static Stream<Arguments> keyRangeOperations() {
        return Stream.of(
                Arguments.of("fetch", (Function<SessionStoreProxy, Object>) p -> p.fetch("from", "to")),
                Arguments.of("backwardFetch", (Function<SessionStoreProxy, Object>) p -> p.backwardFetch("from", "to")),
                Arguments.of("findSessions", (Function<SessionStoreProxy, Object>) p -> p.findSessions("from", "to", 0L, 10L)),
                Arguments.of("backwardFindSessions", (Function<SessionStoreProxy, Object>) p -> p.backwardFindSessions("from", "to", 0L, 10L)));
    }

    @ParameterizedTest(name = "{0} by key returns iterator proxy")
    @MethodSource("singleKeyOperations")
    void singleKeyOperationsReturnIteratorProxy(String name, Function<SessionStoreProxy, Object> operation) {
        lenient().when(delegate.fetch(any())).thenReturn(iterator);
        lenient().when(delegate.backwardFetch(any())).thenReturn(iterator);
        lenient().when(delegate.findSessions(any(), anyLong(), anyLong())).thenReturn(iterator);
        lenient().when(delegate.backwardFindSessions(any(), anyLong(), anyLong())).thenReturn(iterator);
        assertThat(operation.apply(proxy())).isInstanceOf(KeyValueIteratorProxy.class);
    }

    @ParameterizedTest(name = "{0} by key range returns iterator proxy")
    @MethodSource("keyRangeOperations")
    void keyRangeOperationsReturnIteratorProxy(String name, Function<SessionStoreProxy, Object> operation) {
        lenient().when(delegate.fetch(any(), any())).thenReturn(iterator);
        lenient().when(delegate.backwardFetch(any(), any())).thenReturn(iterator);
        lenient().when(delegate.findSessions(any(), any(), anyLong(), anyLong())).thenReturn(iterator);
        lenient().when(delegate.backwardFindSessions(any(), any(), anyLong(), anyLong())).thenReturn(iterator);
        assertThat(operation.apply(proxy())).isInstanceOf(KeyValueIteratorProxy.class);
    }

    @Test
    void fetchSessionReturnsConvertedValue() {
        lenient().when(delegate.fetchSession("key", 0L, 10L)).thenReturn("aggregate");
        assertThat(proxy().fetchSession("key", 0L, 10L)).isInstanceOfSatisfying(Value.class,
                value -> assertThat(value.asString()).isEqualTo("aggregate"));
    }

    private static DataObject windowedKey() {
        final var windowed = new Windowed<>(new DataString("key"), new SessionWindow(0L, 10L));
        return new DataObjectFlattener().toDataObject(windowed);
    }

    @Test
    void putPreservesWindowBounds() {
        proxy().put(windowedKey(), "aggregate");
        final ArgumentCaptor<Windowed<Object>> captor = ArgumentCaptor.captor();
        verify(delegate).put(captor.capture(), any());
        final Windowed<Object> stored = captor.getValue();
        assertThat(stored.window().start()).isZero();
        assertThat(stored.window().end()).isEqualTo(10L);
    }

    @Test
    void removeDeletesWindowedKey() {
        proxy().remove(windowedKey());
        verify(delegate).remove(any());
    }

    @Test
    void putIgnoresNonWindowedKey() {
        proxy().put("plainKey", "aggregate");
        verify(delegate, never()).put(any(), any());
    }
}
