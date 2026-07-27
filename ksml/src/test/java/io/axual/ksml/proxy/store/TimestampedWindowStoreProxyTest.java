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
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TimestampedWindowStoreProxyTest {

    @Mock
    private TimestampedWindowStore<Object, Object> delegate;

    private TimestampedWindowStoreProxy proxy() {
        return new TimestampedWindowStoreProxy(delegate);
    }

    @Test
    @DisplayName("fetch exposes the value and timestamp of the fetched record as a dict")
    void fetchConvertsResultToDict() {
        when(delegate.fetch("key", 5L)).thenReturn(ValueAndTimestamp.make("value", 5L));
        assertThat(proxy().fetch("key", 5L)).isInstanceOf(PythonDict.class)
                .asString().contains("value").contains("5");
    }

    @Test
    @DisplayName("put forwards an existing ValueAndTimestamp unchanged to the delegate")
    void putForwardsValueAndTimestamp() {
        final ValueAndTimestamp<Object> value = ValueAndTimestamp.make("value", 5L);
        proxy().put("key", value, 100L);
        verify(delegate).put("key", value, 100L);
    }

    @Test
    @DisplayName("put wraps a raw value with the record timestamp before storing at the window time")
    void putWrapsRawValueWithTimestamp() {
        proxy().put("key", "value", 100L, 5L);
        final ArgumentCaptor<ValueAndTimestamp<Object>> captor = ArgumentCaptor.captor();
        verify(delegate).put(eq("key"), captor.capture(), eq(100L));
        assertThat(captor.getValue().value()).isEqualTo("value");
        assertThat(captor.getValue().timestamp()).isEqualTo(5L);
    }
}
