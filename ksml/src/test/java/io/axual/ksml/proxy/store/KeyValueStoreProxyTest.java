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

import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KeyValueStoreProxyTest {

    @Mock
    private KeyValueStore<Object, Object> delegate;

    @Mock
    private KeyValueIterator<Object, Object> iterator;

    private KeyValueStoreProxy proxy() {
        return new KeyValueStoreProxy(delegate);
    }

    @Test
    @DisplayName("get delegates to the store and returns the value as a Python value")
    void getConvertsKeyAndReturnsResult() {
        when(delegate.get("key")).thenReturn("value");
        assertThat(proxy().get("key")).isInstanceOfSatisfying(Value.class,
                value -> assertThat(value.asString()).isEqualTo("value"));
        verify(delegate).get("key");
    }

    @Test
    @DisplayName("put forwards the key and value to the delegate store")
    void putForwardsToDelegate() {
        proxy().put("key", "value");
        verify(delegate).put("key", "value");
    }

    @Test
    @DisplayName("delete forwards to the delegate and returns the removed value as a Python value")
    void deleteForwardsToDelegate() {
        when(delegate.delete("key")).thenReturn("old");
        assertThat(proxy().delete("key")).isInstanceOfSatisfying(Value.class,
                value -> assertThat(value.asString()).isEqualTo("old"));
        verify(delegate).delete("key");
    }

    @Test
    @DisplayName("putIfAbsent forwards the key and value to the delegate store")
    void putIfAbsentForwardsToDelegate() {
        when(delegate.putIfAbsent("key", "value")).thenReturn(null);
        proxy().putIfAbsent("key", "value");
        verify(delegate).putIfAbsent("key", "value");
    }

    @Test
    @DisplayName("approximateNumEntries returns the delegate count as a Python value")
    void approximateNumEntriesIsConverted() {
        when(delegate.approximateNumEntries()).thenReturn(5L);
        assertThat(proxy().approximateNumEntries()).isInstanceOfSatisfying(Value.class,
                value -> assertThat(value.asLong()).isEqualTo(5L));
    }

    @Test
    @DisplayName("all wraps the delegate iterator in a KeyValueIteratorProxy")
    void allWrapsIteratorInProxy() {
        when(delegate.all()).thenReturn(iterator);
        assertThat(proxy().all()).isInstanceOf(KeyValueIteratorProxy.class);
    }

    @Test
    @DisplayName("name delegates to the wrapped store")
    void nameDelegates() {
        when(delegate.name()).thenReturn("myStore");
        assertThat(proxy().name()).isEqualTo("myStore");
    }

    @Test
    @DisplayName("persistent and isOpen delegate to the wrapped store")
    void persistentAndIsOpenDelegate() {
        when(delegate.persistent()).thenReturn(true);
        when(delegate.isOpen()).thenReturn(true);
        assertThat(proxy().persistent()).isTrue();
        assertThat(proxy().isOpen()).isTrue();
    }

    @Test
    @DisplayName("flush and close delegate to the wrapped store")
    void flushAndCloseDelegate() {
        final var proxy = proxy();
        proxy.flush();
        proxy.close();
        verify(delegate).flush();
        verify(delegate).close();
    }

    @Test
    @DisplayName("init forwards the context and root store to the wrapped store")
    void initDelegates() {
        final var context = mock(StateStoreContext.class);
        final var root = mock(StateStore.class);
        proxy().init(context, root);
        verify(delegate).init(context, root);
    }

    @Test
    @DisplayName("toString includes the proxy type and the store name")
    void toStringIncludesName() {
        when(delegate.name()).thenReturn("myStore");
        assertThat(proxy()).asString().contains("KeyValueStoreProxy").contains("myStore");
    }
}
