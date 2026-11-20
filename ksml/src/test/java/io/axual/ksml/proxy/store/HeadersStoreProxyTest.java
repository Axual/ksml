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
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.state.AggregationWithHeaders;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStoreWithHeaders;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Python uses headers-aware stores with the same methods and results as timestamped stores (keyValue and window) and
 * session stores. Reads return values, never headers. Writes store no headers, also while a record is
 * being processed (AXPD-12445).
 */
@ExtendWith(MockitoExtension.class)
class HeadersStoreProxyTest {
    private static final long RECORD_TIMESTAMP = 1234L;

    private static PythonContext pythonContext;

    @Mock
    private TimestampedKeyValueStoreWithHeaders<Object, Object> keyValueStore;

    @Mock
    private TimestampedWindowStoreWithHeaders<Object, Object> windowStore;

    @Mock
    private SessionStoreWithHeaders<Object, Object> sessionStore;

    @BeforeAll
    static void setUpContext() {
        pythonContext = new PythonContext(PythonContextConfig.builder().build());
    }

    @AfterAll
    static void tearDownContext() {
        pythonContext.close();
    }

    private static Headers traceHeaders() {
        return new RecordHeaders().add("trace", "abc".getBytes(StandardCharsets.UTF_8));
    }

    private static Object wrap(Object store) {
        return ProxyUtil.wrapStateStore((org.apache.kafka.streams.processor.StateStore) store, null, () -> RECORD_TIMESTAMP);
    }

    private static List<String> runPython(Object proxy, String code) {
        return StoreProxyTestUtil.runPython(pythonContext.context(), proxy, code);
    }

    // --- overloads -------------------------------------------------------------------------------

    @Test
    @DisplayName("the overloads without record headers wrap headers-aware stores the same way, with reads that return values")
    void overloadsWithoutHeadersWrapHeadersStores() {
        final var store = (org.apache.kafka.streams.processor.StateStore) keyValueStore;
        assertThat(ProxyUtil.wrapStateStore(store)).isInstanceOf(TimestampedKeyValueStoreProxy.class);
        assertThat(ProxyUtil.wrapStateStore(store, null)).isInstanceOf(TimestampedKeyValueStoreProxy.class);
        assertThat(ProxyUtil.wrapStateStore((org.apache.kafka.streams.processor.StateStore) windowStore, null)).isInstanceOf(TimestampedWindowStoreProxy.class);
        assertThat(ProxyUtil.wrapStateStore((org.apache.kafka.streams.processor.StateStore) sessionStore, null)).isInstanceOf(SessionStoreProxy.class);

        when(keyValueStore.get("key")).thenReturn(ValueTimestampHeaders.make("value", 100L, traceHeaders()));
        assertThat(runPython(ProxyUtil.wrapStateStore(store, null, () -> RECORD_TIMESTAMP), """
                [store.get("key")]
                """)).containsExactly("value");
    }

    @Test
    @DisplayName("a write through an overload without record headers stores no headers")
    @SuppressWarnings("unchecked")
    void overloadWithoutHeadersPutHasNoHeaders() {
        runPython(ProxyUtil.wrapStateStore((org.apache.kafka.streams.processor.StateStore) keyValueStore, null), """
                store.put("key", "value", 500)
                []
                """);

        final ArgumentCaptor<ValueTimestampHeaders<Object>> captor = ArgumentCaptor.captor();
        verify(keyValueStore).put(eq(new DataString("key")), captor.capture());
        assertThat(captor.getValue().timestamp()).isEqualTo(500L);
        assertThat(captor.getValue().headers().toArray()).isEmpty();
    }

    // --- keyValue ---------------------------------------------------------------------------------

    @Test
    @DisplayName("get returns only the value, and getWithTimestamp the value and timestamp, never headers")
    void keyValueReadsReturnValues() {
        when(keyValueStore.get("key")).thenReturn(ValueTimestampHeaders.make("value", 100L, traceHeaders()));
        assertThat(runPython(wrap(keyValueStore), """
                [store.get("key"), str(store.getWithTimestamp("key"))]
                """)).containsExactly("value", "{'timestamp': 100, 'value': 'value'}");
    }

    @Test
    @DisplayName("all returns entries with key and value, never headers")
    @SuppressWarnings({"unchecked", "rawtypes"})
    void keyValueAllReturnsEntries() {
        final List<KeyValue<Object, Object>> entries = List.of(KeyValue.pair("a", ValueTimestampHeaders.make("value-a", 100L, traceHeaders())));
        when(keyValueStore.all()).thenReturn((KeyValueIterator) new FakeKeyValueIterator<>(entries));
        assertThat(runPython(wrap(keyValueStore), """
                [str(e) for e in store.all()]
                """)).containsExactly("{'key': 'a', 'value': 'value-a'}");
    }

    @Test
    @DisplayName("put stores the value with the record timestamp and no headers, as Kafka Streams' aggregations do")
    @SuppressWarnings("unchecked")
    void keyValuePutStoresNoHeaders() {
        runPython(wrap(keyValueStore), """
                store.put("key", "value")
                []
                """);

        final ArgumentCaptor<ValueTimestampHeaders<Object>> captor = ArgumentCaptor.captor();
        verify(keyValueStore).put(eq(new DataString("key")), captor.capture());
        assertThat(captor.getValue().value()).isEqualTo(new DataString("value"));
        assertThat(captor.getValue().timestamp()).isEqualTo(RECORD_TIMESTAMP);
        assertThat(captor.getValue().headers().toArray()).isEmpty();
    }

    @Test
    @DisplayName("a write without a record being processed stores no headers")
    @SuppressWarnings("unchecked")
    void keyValuePutWithoutRecordHasNoHeaders() {
        runPython(wrap(keyValueStore), """
                store.put("key", "value", 500)
                []
                """);

        final ArgumentCaptor<ValueTimestampHeaders<Object>> captor = ArgumentCaptor.captor();
        verify(keyValueStore).put(eq(new DataString("key")), captor.capture());
        assertThat(captor.getValue().timestamp()).isEqualTo(500L);
        assertThat(captor.getValue().headers().toArray()).isEmpty();
    }

    @Test
    @DisplayName("delete returns only the old value")
    void keyValueDeleteReturnsValue() {
        when(keyValueStore.delete("key")).thenReturn(ValueTimestampHeaders.make("old", 100L, traceHeaders()));
        assertThat(runPython(wrap(keyValueStore), """
                [store.delete("key")]
                """)).containsExactly("old");
    }

    // --- window -----------------------------------------------------------------------------------

    @Test
    @DisplayName("window fetch returns only the value, and fetchWithTimestamp the value and timestamp")
    void windowReadsReturnValues() {
        when(windowStore.fetch("key", 1000L)).thenReturn(ValueTimestampHeaders.make("value", 100L, traceHeaders()));
        assertThat(runPython(wrap(windowStore), """
                [store.fetch("key", 1000), str(store.fetchWithTimestamp("key", 1000))]
                """)).containsExactly("value", "{'timestamp': 100, 'value': 'value'}");
    }

    @Test
    @DisplayName("window put stores the value with the record timestamp and no headers")
    @SuppressWarnings("unchecked")
    void windowPutStoresNoHeaders() {
        runPython(wrap(windowStore), """
                store.put("key", "value", 1000)
                []
                """);

        final ArgumentCaptor<ValueTimestampHeaders<Object>> captor = ArgumentCaptor.captor();
        verify(windowStore).put(eq("key"), captor.capture(), eq(1000L));
        assertThat(captor.getValue().value()).isEqualTo("value");
        assertThat(captor.getValue().timestamp()).isEqualTo(RECORD_TIMESTAMP);
        assertThat(captor.getValue().headers().toArray()).isEmpty();
    }

    // --- session ----------------------------------------------------------------------------------

    @Test
    @DisplayName("session fetchSession returns only the aggregate")
    void sessionFetchSessionReturnsAggregate() {
        when(sessionStore.fetchSession("key", 10L, 20L)).thenReturn(AggregationWithHeaders.make("agg", traceHeaders()));
        assertThat(runPython(wrap(sessionStore), """
                [store.fetchSession("key", 10, 20)]
                """)).containsExactly("agg");
    }

    @Test
    @DisplayName("session put stores the aggregate with no headers")
    @SuppressWarnings("unchecked")
    void sessionPutStoresNoHeaders() {
        runPython(wrap(sessionStore), """
                store.put({"key": "key", "start": 10, "end": 20}, "agg")
                []
                """);

        final ArgumentCaptor<AggregationWithHeaders<Object>> captor = ArgumentCaptor.captor();
        verify(sessionStore).put(eq(new Windowed<>("key", new SessionWindow(10L, 20L))), captor.capture());
        assertThat(captor.getValue().aggregation()).isEqualTo("agg");
        assertThat(captor.getValue().headers().toArray()).isEmpty();
    }
}
