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
import io.axual.ksml.python.PythonContext;
import io.axual.ksml.python.PythonContextConfig;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
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
    private static PythonContext pythonContext;

    @BeforeAll
    static void setUpContext() {
        pythonContext = new PythonContext(PythonContextConfig.builder().build());
    }

    @AfterAll
    static void tearDownContext() {
        pythonContext.close();
    }

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
    @DisplayName("single-key session lookups wrap the delegate result in a StoreIteratorProxy")
    @MethodSource("singleKeyOperations")
    void singleKeyOperationsReturnIteratorProxy(String name, Function<SessionStoreProxy, Object> operation) {
        lenient().when(delegate.fetch(any())).thenReturn(iterator);
        lenient().when(delegate.backwardFetch(any())).thenReturn(iterator);
        lenient().when(delegate.findSessions(any(), anyLong(), anyLong())).thenReturn(iterator);
        lenient().when(delegate.backwardFindSessions(any(), anyLong(), anyLong())).thenReturn(iterator);
        assertThat(operation.apply(proxy())).isInstanceOf(StoreIteratorProxy.class);
    }

    @ParameterizedTest(name = "{0} by key range returns iterator proxy")
    @DisplayName("key-range session lookups wrap the delegate result in a StoreIteratorProxy")
    @MethodSource("keyRangeOperations")
    void keyRangeOperationsReturnIteratorProxy(String name, Function<SessionStoreProxy, Object> operation) {
        lenient().when(delegate.fetch(any(), any())).thenReturn(iterator);
        lenient().when(delegate.backwardFetch(any(), any())).thenReturn(iterator);
        lenient().when(delegate.findSessions(any(), any(), anyLong(), anyLong())).thenReturn(iterator);
        lenient().when(delegate.backwardFindSessions(any(), any(), anyLong(), anyLong())).thenReturn(iterator);
        assertThat(operation.apply(proxy())).isInstanceOf(StoreIteratorProxy.class);
    }

    @Test
    @DisplayName("fetchSession converts the delegate aggregate into a polyglot Value")
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
    @DisplayName("put keeps the windowed key session start and end bounds intact")
    void putPreservesWindowBounds() {
        proxy().put(windowedKey(), "aggregate");
        final ArgumentCaptor<Windowed<Object>> captor = ArgumentCaptor.captor();
        verify(delegate).put(captor.capture(), any());
        final Windowed<Object> stored = captor.getValue();
        assertThat(stored.window().start()).isZero();
        assertThat(stored.window().end()).isEqualTo(10L);
    }

    @Test
    @DisplayName("remove forwards the windowed key deletion to the delegate")
    void removeDeletesWindowedKey() {
        proxy().remove(windowedKey());
        verify(delegate).remove(any());
    }

    /** Runs Python code with {@code store} bound to the proxy; returns "ok", or "ErrorType: message". */
    private String run(String call) {
        pythonContext.context().getBindings("python").putMember("store", proxy());
        return pythonContext.context().eval("python", """
                def run():
                    try:
                        %s
                        return "ok"
                    except Exception as e:
                        return type(e).__name__ + ": " + str(e)
                run()
                """.formatted(call)).asString();
    }

    private Windowed<Object> storedSessionKey() {
        final ArgumentCaptor<Windowed<Object>> captor = ArgumentCaptor.captor();
        verify(delegate).put(captor.capture(), any());
        return captor.getValue();
    }

    @Test
    @DisplayName("put accepts session times in the int range")
    void putWithSmallSessionTimes() {
        assertThat(run("store.put({'key': 'a', 'start': 10, 'end': 20}, 'v')")).isEqualTo("ok");
        final var stored = storedSessionKey();
        assertThat(stored.key()).isEqualTo("a");
        assertThat(stored.window().start()).isEqualTo(10L);
        assertThat(stored.window().end()).isEqualTo(20L);
    }

    @Test
    @DisplayName("put accepts a single-record session, where start equals end")
    void putWithSingleRecordSession() {
        assertThat(run("store.put({'key': 'a', 'start': 3000000000, 'end': 3000000000}, 'v')")).isEqualTo("ok");
        final var stored = storedSessionKey();
        assertThat(stored.window().start()).isEqualTo(3000000000L);
        assertThat(stored.window().end()).isEqualTo(3000000000L);
    }

    @Test
    @DisplayName("remove accepts a session key as reads return it, with startTime and endTime")
    void removeWithSessionKeyFromRead() {
        assertThat(run("store.remove({'key': 'a', 'start': 10, 'end': 10, 'startTime': 'x', 'endTime': 'y'})")).isEqualTo("ok");
        final ArgumentCaptor<Windowed<Object>> captor = ArgumentCaptor.captor();
        verify(delegate).remove(captor.capture());
        assertThat(captor.getValue().window().start()).isEqualTo(10L);
        assertThat(captor.getValue().window().end()).isEqualTo(10L);
    }

    @Test
    @DisplayName("put and remove raise TypeError or ValueError for a malformed session key, and write nothing")
    void malformedSessionKeys() {
        lenient().when(delegate.name()).thenReturn("sessions");
        assertThat(run("store.put('a', 'v')"))
                .isEqualTo("TypeError: Store sessions: session key must be a dict with \"key\", \"start\" and \"end\", got str");
        assertThat(run("store.remove(5)"))
                .isEqualTo("TypeError: Store sessions: session key must be a dict with \"key\", \"start\" and \"end\", got int");
        assertThat(run("store.put({'key': 'a', 'start': 10}, 'v')"))
                .isEqualTo("ValueError: Store sessions: session key must have \"key\", \"start\" and \"end\"");
        assertThat(run("store.put({'key': 'a', 'start': '10', 'end': 20}, 'v')"))
                .isEqualTo("TypeError: Store sessions: session key start must be int, got str");
        assertThat(run("store.put({'key': 'a', 'start': 10, 'end': 20.0}, 'v')"))
                .isEqualTo("TypeError: Store sessions: session key end must be int, got float");
        assertThat(run("store.put({'key': 'a', 'start': 10, 'end': 10 ** 30}, 'v')"))
                .isEqualTo("ValueError: Store sessions: session key end int is out of range for long");
        assertThat(run("store.put({'key': 'a', 'start': 20, 'end': 10}, 'v')"))
                .isEqualTo("ValueError: Store sessions: session key start must not be after end");
        verify(delegate, never()).put(any(), any());
        verify(delegate, never()).remove(any());
    }
}
