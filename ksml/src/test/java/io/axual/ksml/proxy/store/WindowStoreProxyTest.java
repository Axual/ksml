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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
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

    @Test
    void fetchByKeyAndTimeRangeReturnsIteratorProxy() {
        lenient().when(delegate.fetch("key", 0L, 10L)).thenReturn(windowIterator);
        assertThat(proxy().fetch("key", 0L, 10L)).isInstanceOf(WindowStoreIteratorProxy.class);
    }

    @Test
    void fetchByKeyRangeReturnsIteratorProxy() {
        lenient().when(delegate.fetch("from", "to", 0L, 10L)).thenReturn(keyValueIterator);
        assertThat(proxy().fetch("from", "to", 0L, 10L)).isInstanceOf(KeyValueIteratorProxy.class);
    }

    @Test
    void backwardFetchByKeyReturnsIteratorProxy() {
        lenient().when(delegate.backwardFetch("key", 0L, 10L)).thenReturn(windowIterator);
        assertThat(proxy().backwardFetch("key", 0L, 10L)).isInstanceOf(WindowStoreIteratorProxy.class);
    }

    @Test
    void backwardFetchByKeyRangeReturnsIteratorProxy() {
        lenient().when(delegate.backwardFetch("from", "to", 0L, 10L)).thenReturn(keyValueIterator);
        assertThat(proxy().backwardFetch("from", "to", 0L, 10L)).isInstanceOf(KeyValueIteratorProxy.class);
    }

    @Test
    void allAndBackwardAllReturnIteratorProxies() {
        lenient().when(delegate.all()).thenReturn(keyValueIterator);
        lenient().when(delegate.backwardAll()).thenReturn(keyValueIterator);
        assertThat(proxy().all()).isInstanceOf(KeyValueIteratorProxy.class);
        assertThat(proxy().backwardAll()).isInstanceOf(KeyValueIteratorProxy.class);
    }

    @Test
    void fetchAllAndBackwardFetchAllReturnIteratorProxies() {
        lenient().when(delegate.fetchAll(0L, 10L)).thenReturn(keyValueIterator);
        lenient().when(delegate.backwardFetchAll(0L, 10L)).thenReturn(keyValueIterator);
        assertThat(proxy().fetchAll(0L, 10L)).isInstanceOf(KeyValueIteratorProxy.class);
        assertThat(proxy().backwardFetchAll(0L, 10L)).isInstanceOf(KeyValueIteratorProxy.class);
    }

    @Test
    void fetchByKeyAndSingleTimeReturnsValue() {
        lenient().when(delegate.fetch("key", 5L)).thenReturn("value");
        assertThat(proxy().fetch("key", 5L)).isNotNull();
    }

    @Test
    void putForwardsToDelegate() {
        proxy().put("key", "value", 100L);
        verify(delegate).put("key", "value", 100L);
    }
}
