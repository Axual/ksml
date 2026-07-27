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

import io.axual.ksml.data.object.DataString;
import io.axual.ksml.python.PythonDict;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.VersionedRecord;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ProxyUtilTest {

    @Test
    @DisplayName("a null input converts to null")
    void nullConvertsToNull() {
        assertThat(ProxyUtil.toPython(null)).isNull();
    }

    @Test
    @DisplayName("a plain value converts to a Python value")
    void plainValueConvertsToPythonValue() {
        assertThat(ProxyUtil.toPython("value")).isInstanceOfSatisfying(Value.class,
                value -> assertThat(value.asString()).isEqualTo("value"));
    }

    @Test
    @DisplayName("a value-and-timestamp converts to a dict holding the value and timestamp")
    void valueAndTimestampConvertsToDict() {
        final var vat = ValueAndTimestamp.make("value", 100L);
        assertThat(ProxyUtil.toPython(vat)).isInstanceOf(PythonDict.class)
                .asString().contains("value").contains("100");
    }

    @Test
    @DisplayName("a key-value pair converts to a dict holding the key and value")
    void keyValueConvertsToDict() {
        assertThat(ProxyUtil.toPython(new KeyValue<>("key", "value"))).isInstanceOf(PythonDict.class)
                .asString().contains("key").contains("value");
    }

    @Test
    @DisplayName("a versioned record converts to a dict holding the value, timestamp and validTo")
    void versionedRecordConvertsToDict() {
        final VersionedRecord<Object> versionedRecord = mock();
        when(versionedRecord.value()).thenReturn("value");
        when(versionedRecord.timestamp()).thenReturn(100L);
        when(versionedRecord.validTo()).thenReturn(Optional.of(200L));
        assertThat(ProxyUtil.toPython(versionedRecord)).isInstanceOf(PythonDict.class)
                .asString().contains("value").contains("100").contains("200");
    }

    @Test
    @DisplayName("a key-value iterator is wrapped in a proxy that delegates to it")
    void keyValueIteratorWrapsInProxyAndDelegates() {
        final KeyValueIterator<Object, Object> iterator = mock();
        when(iterator.hasNext()).thenReturn(true);
        final var proxy = ProxyUtil.toPython(iterator);
        assertThat(proxy).isInstanceOf(KeyValueIteratorProxy.class);
        assertThat(((KeyValueIteratorProxy) proxy).hasNext()).isTrue();
        verify(iterator).hasNext();
    }

    @Test
    @DisplayName("a window-store iterator is wrapped in a proxy that delegates to it")
    void windowStoreIteratorWrapsInProxyAndDelegates() {
        final WindowStoreIterator<Object> iterator = mock();
        when(iterator.hasNext()).thenReturn(true);
        final var proxy = ProxyUtil.toPython(iterator);
        assertThat(proxy).isInstanceOf(WindowStoreIteratorProxy.class);
        assertThat(((WindowStoreIteratorProxy) proxy).hasNext()).isTrue();
        verify(iterator).hasNext();
    }

    @Test
    @DisplayName("a data object converts to a Python value")
    void dataObjectConvertsToPython() {
        assertThat(ProxyUtil.toPython(new DataString("value"))).isInstanceOfSatisfying(Value.class,
                value -> assertThat(value.asString()).isEqualTo("value"));
    }

    @Test
    @DisplayName("a windowed key converts to a Python value")
    void windowedKeyConvertsToPython() {
        final var windowed = new Windowed<>("key", new SessionWindow(0L, 10L));
        assertThat(ProxyUtil.toPython(windowed)).isInstanceOf(Value.class);
    }
}
