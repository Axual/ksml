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
 * session stores. Reads return values, never headers. Writes keep the headers of the record that is being processed,
 * or no headers when there is none (AXPD-12445).
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

    private static Object wrap(Object store, Headers recordHeaders) {
        return ProxyUtil.wrapStateStore((org.apache.kafka.streams.processor.StateStore) store, null, () -> RECORD_TIMESTAMP, () -> recordHeaders);
    }

    private static List<String> runPython(Object proxy, String code) {
        return StoreProxyTestUtil.runPython(pythonContext.context(), proxy, code);
    }

    private static String headerValue(Headers headers, String key) {
        final var header = headers.lastHeader(key);
        return header != null ? new String(header.value(), StandardCharsets.UTF_8) : null;
    }

    // --- keyValue ---------------------------------------------------------------------------------

    @Test
    @DisplayName("get returns only the value, and getWithTimestamp the value and timestamp, never headers")
    void keyValueReadsReturnValues() {
        when(keyValueStore.get("key")).thenReturn(ValueTimestampHeaders.make("value", 100L, traceHeaders()));
        assertThat(runPython(wrap(keyValueStore, new RecordHeaders()), """
                [store.get("key"), str(store.getWithTimestamp("key"))]
                """)).containsExactly("value", "{'value': 'value', 'timestamp': 100}");
    }

    @Test
    @DisplayName("all returns entries with key and value, never headers")
    @SuppressWarnings({"unchecked", "rawtypes"})
    void keyValueAllReturnsEntries() {
        final List<KeyValue<Object, Object>> entries = List.of(KeyValue.pair("a", ValueTimestampHeaders.make("value-a", 100L, traceHeaders())));
        when(keyValueStore.all()).thenReturn((KeyValueIterator) new FakeKeyValueIterator<>(entries));
        assertThat(runPython(wrap(keyValueStore, new RecordHeaders()), """
                [str(e) for e in store.all()]
                """)).containsExactly("{'key': 'a', 'value': 'value-a'}");
    }

    @Test
    @DisplayName("put stores the value with the record timestamp and a copy of the record headers")
    @SuppressWarnings("unchecked")
    void keyValuePutKeepsRecordHeaders() {
        final var recordHeaders = traceHeaders();
        runPython(wrap(keyValueStore, recordHeaders), """
                store.put("key", "value")
                []
                """);
        recordHeaders.add("later", "x".getBytes(StandardCharsets.UTF_8));

        final ArgumentCaptor<ValueTimestampHeaders<Object>> captor = ArgumentCaptor.captor();
        verify(keyValueStore).put(eq(new DataString("key")), captor.capture());
        assertThat(captor.getValue().value()).isEqualTo(new DataString("value"));
        assertThat(captor.getValue().timestamp()).isEqualTo(RECORD_TIMESTAMP);
        assertThat(headerValue(captor.getValue().headers(), "trace")).isEqualTo("abc");
        assertThat(captor.getValue().headers().lastHeader("later")).isNull();
    }

    @Test
    @DisplayName("a write without a record being processed stores no headers")
    @SuppressWarnings("unchecked")
    void keyValuePutWithoutRecordHasNoHeaders() {
        runPython(wrap(keyValueStore, new RecordHeaders()), """
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
        assertThat(runPython(wrap(keyValueStore, new RecordHeaders()), """
                [store.delete("key")]
                """)).containsExactly("old");
    }

    // --- window -----------------------------------------------------------------------------------

    @Test
    @DisplayName("window fetch returns only the value, and fetchWithTimestamp the value and timestamp")
    void windowReadsReturnValues() {
        when(windowStore.fetch("key", 1000L)).thenReturn(ValueTimestampHeaders.make("value", 100L, traceHeaders()));
        assertThat(runPython(wrap(windowStore, new RecordHeaders()), """
                [store.fetch("key", 1000), str(store.fetchWithTimestamp("key", 1000))]
                """)).containsExactly("value", "{'value': 'value', 'timestamp': 100}");
    }

    @Test
    @DisplayName("window put stores the value with the record timestamp and the record headers")
    @SuppressWarnings("unchecked")
    void windowPutKeepsRecordHeaders() {
        runPython(wrap(windowStore, traceHeaders()), """
                store.put("key", "value", 1000)
                []
                """);

        final ArgumentCaptor<ValueTimestampHeaders<Object>> captor = ArgumentCaptor.captor();
        verify(windowStore).put(eq("key"), captor.capture(), eq(1000L));
        assertThat(captor.getValue().value()).isEqualTo("value");
        assertThat(captor.getValue().timestamp()).isEqualTo(RECORD_TIMESTAMP);
        assertThat(headerValue(captor.getValue().headers(), "trace")).isEqualTo("abc");
    }

    // --- session ----------------------------------------------------------------------------------

    @Test
    @DisplayName("session fetchSession returns only the aggregate")
    void sessionFetchSessionReturnsAggregate() {
        when(sessionStore.fetchSession("key", 10L, 20L)).thenReturn(AggregationWithHeaders.make("agg", traceHeaders()));
        assertThat(runPython(wrap(sessionStore, new RecordHeaders()), """
                [store.fetchSession("key", 10, 20)]
                """)).containsExactly("agg");
    }

    @Test
    @DisplayName("session put stores the aggregate with the record headers")
    @SuppressWarnings("unchecked")
    void sessionPutKeepsRecordHeaders() {
        runPython(wrap(sessionStore, traceHeaders()), """
                store.put({"key": "key", "start": 10, "end": 20}, "agg")
                []
                """);

        final ArgumentCaptor<AggregationWithHeaders<Object>> captor = ArgumentCaptor.captor();
        verify(sessionStore).put(eq(new Windowed<>("key", new SessionWindow(10L, 20L))), captor.capture());
        assertThat(captor.getValue().aggregation()).isEqualTo("agg");
        assertThat(headerValue(captor.getValue().headers(), "trace")).isEqualTo("abc");
    }
}
