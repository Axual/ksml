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

import io.axual.ksml.python.PythonContext;
import io.axual.ksml.python.PythonContextConfig;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TimestampedWindowStoreProxyTest {

    private static final long RECORD_TIMESTAMP = 1234L;

    private static PythonContext pythonContext;

    @Mock
    private TimestampedWindowStore<Object, Object> delegate;

    @BeforeAll
    static void setUpContext() {
        pythonContext = new PythonContext(PythonContextConfig.builder().build());
    }

    @AfterAll
    static void tearDownContext() {
        pythonContext.close();
    }

    private TimestampedWindowStoreProxy proxy() {
        return new TimestampedWindowStoreProxy(delegate, () -> RECORD_TIMESTAMP);
    }

    /** Runs Python code with {@code store} bound to the proxy, and returns the result as a list of strings. */
    private List<String> runPython(String code) {
        return StoreProxyTestUtil.runPython(pythonContext.context(), proxy(), code);
    }

    /** Runs a Python call on {@code store} inside try/except Exception; returns "ok", or "ErrorType: message". */
    private String pythonErrorOf(Object store, String call) {
        return StoreProxyTestUtil.pythonErrorOf(pythonContext.context(), store, call);
    }

    /** A Kafka window store iterator: the key is the window start time 0 or 60000, the n-th entry has timestamp 100 + n. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static WindowStoreIterator<ValueAndTimestamp<Object>> windowEntries(String... values) {
        final var result = new ArrayList<KeyValue<Long, Object>>();
        for (int i = 0; i < values.length; i++)
            result.add(KeyValue.pair(i * 60000L, ValueAndTimestamp.make(values[i], 100L + i)));
        return (WindowStoreIterator) new FakeKeyValueIterator.Window(result);
    }

    /** A Kafka iterator over windowed keys k, all in the window [0, 60000), with value "value-k" and timestamp 100 + n. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static KeyValueIterator<Windowed<Object>, ValueAndTimestamp<Object>> windowedEntries(String... keys) {
        final var result = new ArrayList<KeyValue<Windowed<Object>, Object>>();
        for (int i = 0; i < keys.length; i++)
            result.add(KeyValue.pair(new Windowed<>(keys[i], new TimeWindow(0, 60000)), ValueAndTimestamp.make("value-" + keys[i], 100L + i)));
        return (KeyValueIterator) new FakeKeyValueIterator<>(result);
    }

    /** Python helpers: format entries with or without timestamp, and check that the timestamp is absent or present. */
    private static final String PYTHON_HELPERS = """
            def plain(entries):
                for e in entries:
                    assert sorted(e.keys()) == ["key", "value"], e
                return ",".join(str(e["key"]["key"] if type(e["key"]) is dict else e["key"]) + ":" + e["value"] for e in entries)
            def stamped(entries):
                for e in entries:
                    assert sorted(e.keys()) == ["key", "timestamp", "value"], e
                return ",".join(str(e["key"]["key"] if type(e["key"]) is dict else e["key"]) + ":" + e["value"] + "@" + str(e["timestamp"]) for e in entries)
            """;

    @Test
    @DisplayName("fetch(key, time) returns only the value, like on a plain window store")
    void fetchReturnsOnlyTheValue() {
        when(delegate.fetch("key", 5L)).thenReturn(ValueAndTimestamp.make("value", 7L));
        assertThat(runPython("""
                [store.fetch("key", 5)]
                """)).containsExactly("value");
    }

    @Test
    @DisplayName("fetchWithTimestamp(key, time) returns the value and timestamp as a real dict, or None")
    void fetchWithTimestampReturnsDict() {
        when(delegate.fetch("key", 5L)).thenReturn(ValueAndTimestamp.make("value", 7L));
        assertThat(runPython("""
                result = store.fetchWithTimestamp("key", 5)
                assert type(result) is dict
                assert result == {"value": "value", "timestamp": 7}, result
                [str(store.fetchWithTimestamp("missing", 5))]
                """)).containsExactly("None");
    }

    @Test
    @DisplayName("fetch and backwardFetch of one key over a time range return {key, value} entries keyed by window start")
    void fetchKeyTimeRangeReturnsEntriesWithoutTimestamp() {
        when(delegate.fetch("key", 0L, 60000L)).thenReturn(windowEntries("v1", "v2"));
        when(delegate.backwardFetch("key", 0L, 60000L)).thenReturn(windowEntries("v2", "v1"));
        assertThat(runPython(PYTHON_HELPERS + """
                [plain(list(store.fetch("key", 0, 60000))), plain(list(store.backwardFetch("key", 0, 60000)))]
                """)).containsExactly("0:v1,60000:v2", "0:v2,60000:v1");
    }

    @Test
    @DisplayName("fetchWithTimestamp and backwardFetchWithTimestamp of one key over a time range include the timestamp")
    void fetchKeyTimeRangeWithTimestamp() {
        when(delegate.fetch("key", 0L, 60000L)).thenReturn(windowEntries("v1", "v2"));
        when(delegate.backwardFetch("key", 0L, 60000L)).thenReturn(windowEntries("v2", "v1"));
        assertThat(runPython(PYTHON_HELPERS + """
                [stamped(list(store.fetchWithTimestamp("key", 0, 60000))),
                 stamped(list(store.backwardFetchWithTimestamp("key", 0, 60000)))]
                """)).containsExactly("0:v1@100,60000:v2@101", "0:v2@100,60000:v1@101");
    }

    @Test
    @DisplayName("all, backwardAll, fetchAll, backwardFetchAll and key-range fetch return {key, value} entries with windowed keys")
    void windowedIteratorsReturnEntriesWithoutTimestamp() {
        when(delegate.all()).thenReturn(windowedEntries("a", "b"));
        when(delegate.backwardAll()).thenReturn(windowedEntries("b", "a"));
        when(delegate.fetchAll(0L, 60000L)).thenReturn(windowedEntries("a", "b"));
        when(delegate.backwardFetchAll(0L, 60000L)).thenReturn(windowedEntries("b", "a"));
        when(delegate.fetch("a", "b", 0L, 60000L)).thenReturn(windowedEntries("a", "b"));
        when(delegate.backwardFetch("a", "b", 0L, 60000L)).thenReturn(windowedEntries("b", "a"));
        assertThat(runPython(PYTHON_HELPERS + """
                [plain(list(store.all())), plain(list(store.backwardAll())),
                 plain(list(store.fetchAll(0, 60000))), plain(list(store.backwardFetchAll(0, 60000))),
                 plain(list(store.fetch("a", "b", 0, 60000))), plain(list(store.backwardFetch("a", "b", 0, 60000)))]
                """)).containsExactly("a:value-a,b:value-b", "b:value-b,a:value-a", "a:value-a,b:value-b",
                "b:value-b,a:value-a", "a:value-a,b:value-b", "b:value-b,a:value-a");
    }

    @Test
    @DisplayName("the *WithTimestamp versions of all, fetchAll and key-range fetch include the timestamp")
    void windowedIteratorsWithTimestamp() {
        when(delegate.all()).thenReturn(windowedEntries("a", "b"));
        when(delegate.backwardAll()).thenReturn(windowedEntries("b", "a"));
        when(delegate.fetchAll(0L, 60000L)).thenReturn(windowedEntries("a", "b"));
        when(delegate.backwardFetchAll(0L, 60000L)).thenReturn(windowedEntries("b", "a"));
        when(delegate.fetch("a", "b", 0L, 60000L)).thenReturn(windowedEntries("a", "b"));
        when(delegate.backwardFetch("a", "b", 0L, 60000L)).thenReturn(windowedEntries("b", "a"));
        assertThat(runPython(PYTHON_HELPERS + """
                [stamped(list(store.allWithTimestamp())), stamped(list(store.backwardAllWithTimestamp())),
                 stamped(list(store.fetchAllWithTimestamp(0, 60000))), stamped(list(store.backwardFetchAllWithTimestamp(0, 60000))),
                 stamped(list(store.fetchWithTimestamp("a", "b", 0, 60000))),
                 stamped(list(store.backwardFetchWithTimestamp("a", "b", 0, 60000)))]
                """)).containsExactly("a:value-a@100,b:value-b@101", "b:value-b@100,a:value-a@101",
                "a:value-a@100,b:value-b@101", "b:value-b@100,a:value-a@101",
                "a:value-a@100,b:value-b@101", "b:value-b@100,a:value-a@101");
    }

    @Test
    @DisplayName("windowed keys in entries are dicts with the original key and the window")
    void windowedKeyIsFlattened() {
        when(delegate.all()).thenReturn(windowedEntries("a"));
        assertThat(runPython("""
                e = next(iter(store.all()))
                [e["key"]["key"], str(e["key"]["start"]), str(e["key"]["end"])]
                """)).containsExactly("a", "0", "60000");
    }

    @Test
    @DisplayName("put(key, value, windowStart) without a timestamp stores the value in the window with the record timestamp")
    void putWithoutTimestampFromPython() {
        runPython("""
                store.put("key", "value", 60000)
                []
                """);
        final ArgumentCaptor<ValueAndTimestamp<Object>> captor = ArgumentCaptor.captor();
        verify(delegate).put(eq("key"), captor.capture(), eq(60000L));
        assertThat(captor.getValue().value()).isEqualTo("value");
        assertThat(captor.getValue().timestamp()).isEqualTo(RECORD_TIMESTAMP);
    }

    @Test
    @DisplayName("put with a timestamp stores the value with that timestamp in the window")
    void putWrapsRawValueWithTimestamp() {
        proxy().put("key", "value", 100L, 5L);
        final ArgumentCaptor<ValueAndTimestamp<Object>> captor = ArgumentCaptor.captor();
        verify(delegate).put(eq("key"), captor.capture(), eq(100L));
        assertThat(captor.getValue().value()).isEqualTo("value");
        assertThat(captor.getValue().timestamp()).isEqualTo(5L);
    }

    @Test
    @DisplayName("put with a negative timestamp raises ValueError, and writes nothing")
    void putWithNegativeTimestampIsAnError() {
        assertThat(pythonErrorOf(proxy(), "store.put(\"key\", \"value\", 60000, -1)"))
                .isEqualTo("ValueError: put timestamp must not be negative, got -1");
        verifyNoInteractions(delegate);
    }

    @Test
    @DisplayName("put without a timestamp outside record processing raises ValueError, and writes nothing")
    void putWithoutTimestampOrRecordIsAnError() {
        assertThat(pythonErrorOf(ProxyUtil.wrapStateStore(delegate), "store.put(\"key\", \"value\", 60000)"))
                .startsWith("ValueError: No record is being processed");
        verifyNoInteractions(delegate);
    }

    @Test
    @DisplayName("put with a None value deletes the window entry also outside record processing, as a delete needs no timestamp")
    void deleteWithNoneWithoutRecord() {
        assertThat(pythonErrorOf(ProxyUtil.wrapStateStore(delegate), "store.put(\"key\", None, 60000)")).isEqualTo("ok");
        verify(delegate).put("key", null, 60000L);
    }
}
