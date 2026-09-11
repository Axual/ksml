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
import io.axual.ksml.python.PythonContext;
import io.axual.ksml.python.PythonContextConfig;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.VersionedRecord;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** ProxyUtil.toPython() needs an entered Python context; one is entered once for the whole class. */
class ProxyUtilTest {
    private static PythonContext pythonContext;

    @BeforeAll
    static void enterContext() {
        pythonContext = new PythonContext(PythonContextConfig.builder().build());
        pythonContext.context().enter();
    }

    @AfterAll
    static void leaveContext() {
        pythonContext.context().leave();
        pythonContext.close();
    }

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
    @DisplayName("a value-and-timestamp converts to a real dict holding the value and timestamp")
    void valueAndTimestampConvertsToDict() {
        final var vat = ValueAndTimestamp.make("value", 100L);
        final var result = (Value) ProxyUtil.toPython(vat);
        assertThat(result.getMetaObject().getMetaSimpleName()).isEqualTo("dict");
        assertThat(result.getHashValue("value").asString()).isEqualTo("value");
        assertThat(result.getHashValue("timestamp").asLong()).isEqualTo(100L);
    }

    @Test
    @DisplayName("a key-value pair converts to a real dict holding the key and value")
    void keyValueConvertsToDict() {
        final var result = (Value) ProxyUtil.toPython(new KeyValue<>("key", "value"));
        assertThat(result.getMetaObject().getMetaSimpleName()).isEqualTo("dict");
        assertThat(result.getHashValue("key").asString()).isEqualTo("key");
        assertThat(result.getHashValue("value").asString()).isEqualTo("value");
    }

    @Test
    @DisplayName("a versioned record converts to a real dict holding the value, timestamp and validTo")
    void versionedRecordConvertsToDict() {
        final VersionedRecord<Object> versionedRecord = mock();
        when(versionedRecord.value()).thenReturn("value");
        when(versionedRecord.timestamp()).thenReturn(100L);
        when(versionedRecord.validTo()).thenReturn(Optional.of(200L));
        final var result = (Value) ProxyUtil.toPython(versionedRecord);
        assertThat(result.getMetaObject().getMetaSimpleName()).isEqualTo("dict");
        assertThat(result.getHashValue("value").asString()).isEqualTo("value");
        assertThat(result.getHashValue("timestamp").asLong()).isEqualTo(100L);
        assertThat(result.getHashValue("validTo").asLong()).isEqualTo(200L);
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
    @DisplayName("a windowed key converts to a real dict")
    void windowedKeyConvertsToPython() {
        final var windowed = new Windowed<>("key", new SessionWindow(0L, 10L));
        final var result = (Value) ProxyUtil.toPython(windowed);
        assertThat(result.getMetaObject().getMetaSimpleName()).isEqualTo("dict");
        assertThat(result.getHashValue("key").asString()).isEqualTo("key");
    }
}
