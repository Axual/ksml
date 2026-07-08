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

import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.VersionedRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class VersionedKeyValueStoreProxyTest {

    @Mock
    private VersionedKeyValueStore<Object, Object> delegate;
    @Mock
    private VersionedRecord<Object> versionedRecord;

    private VersionedKeyValueStoreProxy proxy() {
        return new VersionedKeyValueStoreProxy(delegate);
    }

    @Test
    void getByKeyConvertsResult() {
        when(versionedRecord.value()).thenReturn("value");
        when(versionedRecord.timestamp()).thenReturn(100L);
        when(versionedRecord.validTo()).thenReturn(java.util.Optional.empty());
        when(delegate.get("key")).thenReturn(versionedRecord);
        assertThat(proxy().get("key")).isNotNull();
    }

    @Test
    void getByKeyAndTimestampConvertsResult() {
        lenient().when(versionedRecord.value()).thenReturn("value");
        lenient().when(versionedRecord.timestamp()).thenReturn(100L);
        lenient().when(versionedRecord.validTo()).thenReturn(java.util.Optional.empty());
        when(delegate.get("key", 50L)).thenReturn(versionedRecord);
        assertThat(proxy().get("key", 50L)).isNotNull();
    }

    @Test
    void deleteConvertsResult() {
        lenient().when(versionedRecord.value()).thenReturn("value");
        lenient().when(versionedRecord.timestamp()).thenReturn(100L);
        lenient().when(versionedRecord.validTo()).thenReturn(java.util.Optional.empty());
        when(delegate.delete("key", 50L)).thenReturn(versionedRecord);
        assertThat(proxy().delete("key", 50L)).isNotNull();
    }

    @Test
    void putForwardsToDelegate() {
        proxy().put("key", "value", 100L);
        verify(delegate).put(eq("key"), any(), eq(100L));
    }
}
