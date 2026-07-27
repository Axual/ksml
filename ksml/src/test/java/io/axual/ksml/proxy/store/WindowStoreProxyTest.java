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

import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class WindowStoreProxyTest {

    @Mock
    private WindowStore<Object, Object> delegate;
    @Mock
    private WindowStoreIterator<Object> windowIterator;
    @Mock
    private KeyValueIterator<Windowed<Object>, Object> keyValueIterator;

    private WindowStoreProxy proxy() {
        return new WindowStoreProxy(delegate);
    }

    private static Stream<Arguments> singleKeyTimeRangeFetches() {
        return Stream.of(
                Arguments.of("fetch", (Function<WindowStoreProxy, Object>) p -> p.fetch("key", 0L, 10L)),
                Arguments.of("backwardFetch", (Function<WindowStoreProxy, Object>) p -> p.backwardFetch("key", 0L, 10L)));
    }

    private static Stream<Arguments> keyRangeTimeRangeFetches() {
        return Stream.of(
                Arguments.of("fetch", (Function<WindowStoreProxy, Object>) p -> p.fetch("from", "to", 0L, 10L)),
                Arguments.of("backwardFetch", (Function<WindowStoreProxy, Object>) p -> p.backwardFetch("from", "to", 0L, 10L)));
    }

    @ParameterizedTest(name = "{0} by key and time range returns window iterator proxy")
    @DisplayName("single-key time-range fetches wrap the delegate result in a WindowStoreIteratorProxy")
    @MethodSource("singleKeyTimeRangeFetches")
    void singleKeyTimeRangeFetchReturnsWindowIteratorProxy(String name, Function<WindowStoreProxy, Object> operation) {
        lenient().when(delegate.fetch(any(), anyLong(), anyLong())).thenReturn(windowIterator);
        lenient().when(delegate.backwardFetch(any(), anyLong(), anyLong())).thenReturn(windowIterator);
        assertThat(operation.apply(proxy())).isInstanceOf(WindowStoreIteratorProxy.class);
    }

    @ParameterizedTest(name = "{0} by key range and time range returns key/value iterator proxy")
    @DisplayName("key-range time-range fetches wrap the delegate result in a KeyValueIteratorProxy")
    @MethodSource("keyRangeTimeRangeFetches")
    void keyRangeTimeRangeFetchReturnsKeyValueIteratorProxy(String name, Function<WindowStoreProxy, Object> operation) {
        lenient().when(delegate.fetch(any(), any(), anyLong(), anyLong())).thenReturn(keyValueIterator);
        lenient().when(delegate.backwardFetch(any(), any(), anyLong(), anyLong())).thenReturn(keyValueIterator);
        assertThat(operation.apply(proxy())).isInstanceOf(KeyValueIteratorProxy.class);
    }

    @Test
    @DisplayName("all and backwardAll wrap the delegate result in a KeyValueIteratorProxy")
    void allAndBackwardAllReturnIteratorProxies() {
        lenient().when(delegate.all()).thenReturn(keyValueIterator);
        lenient().when(delegate.backwardAll()).thenReturn(keyValueIterator);
        assertThat(proxy().all()).isInstanceOf(KeyValueIteratorProxy.class);
        assertThat(proxy().backwardAll()).isInstanceOf(KeyValueIteratorProxy.class);
    }

    @Test
    @DisplayName("fetchAll and backwardFetchAll wrap the delegate result in a KeyValueIteratorProxy")
    void fetchAllAndBackwardFetchAllReturnIteratorProxies() {
        lenient().when(delegate.fetchAll(0L, 10L)).thenReturn(keyValueIterator);
        lenient().when(delegate.backwardFetchAll(0L, 10L)).thenReturn(keyValueIterator);
        assertThat(proxy().fetchAll(0L, 10L)).isInstanceOf(KeyValueIteratorProxy.class);
        assertThat(proxy().backwardFetchAll(0L, 10L)).isInstanceOf(KeyValueIteratorProxy.class);
    }

    @Test
    @DisplayName("fetch by key and single timestamp returns the value as a polyglot Value")
    void fetchByKeyAndSingleTimeReturnsValue() {
        lenient().when(delegate.fetch("key", 5L)).thenReturn("value");
        assertThat(proxy().fetch("key", 5L)).isInstanceOfSatisfying(Value.class,
                value -> assertThat(value.asString()).isEqualTo("value"));
    }

    @Test
    @DisplayName("put forwards the key, value and window timestamp to the delegate")
    void putForwardsToDelegate() {
        proxy().put("key", "value", 100L);
        verify(delegate).put("key", "value", 100L);
    }
}
