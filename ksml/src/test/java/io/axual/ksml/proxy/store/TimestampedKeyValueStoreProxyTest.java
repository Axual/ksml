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

import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TimestampedKeyValueStoreProxyTest {

    @Mock
    private TimestampedKeyValueStore<Object, Object> delegate;

    private TimestampedKeyValueStoreProxy proxy() {
        return new TimestampedKeyValueStoreProxy(delegate);
    }

    @Test
    void getConvertsKeyAndResult() {
        when(delegate.get("key")).thenReturn(ValueAndTimestamp.make("value", 100L));
        assertThat(proxy().get("key")).isNotNull();
        verify(delegate).get("key");
    }

    @Test
    void deleteConvertsResult() {
        when(delegate.delete("key")).thenReturn(ValueAndTimestamp.make("value", 100L));
        assertThat(proxy().delete("key")).isNotNull();
    }

    @Test
    void putWrapsValueWithTimestamp() {
        proxy().put("key", "value", 100L);
        verify(delegate).put(any(), any());
    }

    @Test
    void putIfAbsentWrapsValueWithTimestamp() {
        when(delegate.putIfAbsent(any(), any())).thenReturn(null);
        proxy().putIfAbsent("key", "value", 100L);
        verify(delegate).putIfAbsent(any(), any());
    }

    @Test
    void approximateNumEntriesIsConverted() {
        when(delegate.approximateNumEntries()).thenReturn(3L);
        assertThat(proxy().approximateNumEntries()).isNotNull();
    }
}
