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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
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

    @Test
    void fetchByKeyReturnsIteratorProxy() {
        lenient().when(delegate.fetch("key")).thenReturn(iterator);
        assertThat(proxy().fetch("key")).isInstanceOf(KeyValueIteratorProxy.class);
    }

    @Test
    void fetchByKeyRangeReturnsIteratorProxy() {
        lenient().when(delegate.fetch("from", "to")).thenReturn(iterator);
        assertThat(proxy().fetch("from", "to")).isInstanceOf(KeyValueIteratorProxy.class);
    }

    @Test
    void backwardFetchByKeyReturnsIteratorProxy() {
        lenient().when(delegate.backwardFetch("key")).thenReturn(iterator);
        assertThat(proxy().backwardFetch("key")).isInstanceOf(KeyValueIteratorProxy.class);
    }

    @Test
    void backwardFetchByKeyRangeReturnsIteratorProxy() {
        lenient().when(delegate.backwardFetch("from", "to")).thenReturn(iterator);
        assertThat(proxy().backwardFetch("from", "to")).isInstanceOf(KeyValueIteratorProxy.class);
    }

    @Test
    void findSessionsByKeyReturnsIteratorProxy() {
        lenient().when(delegate.findSessions("key", 0L, 10L)).thenReturn(iterator);
        assertThat(proxy().findSessions("key", 0L, 10L)).isInstanceOf(KeyValueIteratorProxy.class);
    }

    @Test
    void findSessionsByKeyRangeReturnsIteratorProxy() {
        lenient().when(delegate.findSessions("from", "to", 0L, 10L)).thenReturn(iterator);
        assertThat(proxy().findSessions("from", "to", 0L, 10L)).isInstanceOf(KeyValueIteratorProxy.class);
    }

    @Test
    void backwardFindSessionsByKeyReturnsIteratorProxy() {
        lenient().when(delegate.backwardFindSessions("key", 0L, 10L)).thenReturn(iterator);
        assertThat(proxy().backwardFindSessions("key", 0L, 10L)).isInstanceOf(KeyValueIteratorProxy.class);
    }

    @Test
    void backwardFindSessionsByKeyRangeReturnsIteratorProxy() {
        lenient().when(delegate.backwardFindSessions("from", "to", 0L, 10L)).thenReturn(iterator);
        assertThat(proxy().backwardFindSessions("from", "to", 0L, 10L)).isInstanceOf(KeyValueIteratorProxy.class);
    }

    @Test
    void fetchSessionReturnsConvertedValue() {
        lenient().when(delegate.fetchSession("key", 0L, 10L)).thenReturn("aggregate");
        assertThat(proxy().fetchSession("key", 0L, 10L)).isNotNull();
    }

    private static DataObject windowedKey() {
        final var windowed = new Windowed<>(new DataString("key"), new SessionWindow(0L, 10L));
        return new DataObjectFlattener().toDataObject(windowed);
    }

    @Test
    void putStoresWindowedAggregate() {
        proxy().put(windowedKey(), "aggregate");
        verify(delegate).put(any(), any());
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
