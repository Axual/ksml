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

import io.axual.ksml.python.PythonDict;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KeyValueIteratorProxyTest {

    @Mock
    private KeyValueIterator<Object, Object> iterator;

    private KeyValueIteratorProxy proxy() {
        return new KeyValueIteratorProxy(iterator);
    }

    @Test
    void hasNextDelegates() {
        when(iterator.hasNext()).thenReturn(true);
        assertThat(proxy().hasNext()).isTrue();
    }

    @Test
    void nextConvertsEntryToDict() {
        when(iterator.hasNext()).thenReturn(true);
        when(iterator.next()).thenReturn(new KeyValue<>("key", "value"));
        assertThat(proxy().next()).isInstanceOf(PythonDict.class);
    }

    @Test
    void nextReturnsNullWhenExhausted() {
        when(iterator.hasNext()).thenReturn(false);
        assertThat(proxy().next()).isNull();
    }

    @Test
    void closeIsIdempotent() {
        final var proxy = proxy();
        proxy.close();
        proxy.close();
        verify(iterator, times(1)).close();
    }
}
