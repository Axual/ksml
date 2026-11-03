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
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.graalvm.polyglot.Value;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TimestampedKeyValueStoreProxyTest {
    private static PythonContext pythonContext;

    @Mock
    private TimestampedKeyValueStore<Object, Object> delegate;

    @BeforeAll
    static void setUpContext() {
        pythonContext = new PythonContext(PythonContextConfig.builder().build());
    }

    @AfterAll
    static void tearDownContext() {
        pythonContext.close();
    }

    private static final long RECORD_TIMESTAMP = 1234L;

    private TimestampedKeyValueStoreProxy proxy() {
        return new TimestampedKeyValueStoreProxy(delegate, () -> RECORD_TIMESTAMP);
    }

    /** Runs Python code with {@code store} bound to the proxy, and returns the result as a list of strings. */
    private List<String> runPython(String code) {
        pythonContext.context().getBindings("python").putMember("store", proxy());
        final var result = pythonContext.context().eval("python", code);
        final var strings = new ArrayList<String>();
        for (long i = 0; i < result.getArraySize(); i++) strings.add(result.getArrayElement(i).asString());
        return strings;
    }

    /** Runs Python code inside try/except Exception, and returns "ErrorType: message", or "no error". */
    private String pythonErrorOf(String call) {
        return runPython("""
                def run():
                    try:
                        %s
                        return "no error"
                    except Exception as e:
                        return type(e).__name__ + ": " + str(e)
                [run()]
                """.formatted(call)).get(0);
    }

    /** A Kafka iterator over timestamped entries: key k has value "value-k", the n-th entry has timestamp 100 + n. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static KeyValueIterator<Object, ValueAndTimestamp<Object>> entries(String... keys) {
        final var result = new ArrayList<KeyValue<Object, Object>>();
        for (int i = 0; i < keys.length; i++)
            result.add(KeyValue.pair(keys[i], ValueAndTimestamp.make("value-" + keys[i], 100L + i)));
        return (KeyValueIterator) new FakeKeyValueIterator<>(result);
    }

    private static KeyValue<Object, ValueAndTimestamp<Object>> entry(Object key, Object value, long timestamp) {
        return KeyValue.pair(key, ValueAndTimestamp.make(value, timestamp));
    }

    /** Captures the list of entries that the proxy passed to the delegate putAll. */
    private List<KeyValue<Object, ValueAndTimestamp<Object>>> capturePutAll() {
        final ArgumentCaptor<List<KeyValue<Object, ValueAndTimestamp<Object>>>> captor = ArgumentCaptor.captor();
        verify(delegate).putAll(captor.capture());
        return captor.getValue();
    }

    @Test
    @DisplayName("get returns only the value, like on a plain key-value store")
    void getReturnsOnlyTheValue() {
        when(delegate.get("key")).thenReturn(ValueAndTimestamp.make("value", 100L));
        assertThat(runPython("""
                [store.get("key")]
                """)).containsExactly("value");
        verify(delegate).get("key");
    }

    @Test
    @DisplayName("getWithTimestamp returns the value and timestamp as a real dict")
    void getWithTimestampReturnsDict() {
        when(delegate.get("key")).thenReturn(ValueAndTimestamp.make("value", 100L));
        assertThat(runPython("""
                result = store.getWithTimestamp("key")
                assert type(result) is dict
                assert result == {"value": "value", "timestamp": 100}
                ["ok"]
                """)).containsExactly("ok");
    }

    @Test
    @DisplayName("getWithTimestamp returns None for a missing key")
    void getWithTimestampMissingKeyReturnsNone() {
        assertThat(runPython("""
                [str(store.getWithTimestamp("missing"))]
                """)).containsExactly("None");
    }

    @Test
    @DisplayName("delete returns only the removed value, like on a plain key-value store")
    void deleteReturnsOnlyTheValue() {
        when(delegate.delete("key")).thenReturn(ValueAndTimestamp.make("value", 100L));
        assertThat(runPython("""
                [store.delete("key")]
                """)).containsExactly("value");
    }

    @Test
    @DisplayName("put wraps the raw value with the given timestamp before storing")
    void putWrapsValueWithTimestamp() {
        proxy().put("key", "value", 100L);
        final ArgumentCaptor<ValueAndTimestamp<Object>> captor = ArgumentCaptor.captor();
        verify(delegate).put(any(), captor.capture());
        assertThat(captor.getValue().timestamp()).isEqualTo(100L);
        assertThat(captor.getValue().value()).isNotNull();
    }

    @Test
    @DisplayName("putIfAbsent wraps the raw value with the given timestamp before storing")
    void putIfAbsentWrapsValueWithTimestamp() {
        when(delegate.putIfAbsent(any(), any())).thenReturn(null);
        proxy().putIfAbsent("key", "value", 100L);
        final ArgumentCaptor<ValueAndTimestamp<Object>> captor = ArgumentCaptor.captor();
        verify(delegate).putIfAbsent(any(), captor.capture());
        assertThat(captor.getValue().timestamp()).isEqualTo(100L);
        assertThat(captor.getValue().value()).isNotNull();
    }

    @Test
    @DisplayName("approximateNumEntries returns the delegate count as a polyglot Value")
    void approximateNumEntriesIsConverted() {
        when(delegate.approximateNumEntries()).thenReturn(3L);
        assertThat(proxy().approximateNumEntries()).isInstanceOfSatisfying(Value.class,
                value -> assertThat(value.asLong()).isEqualTo(3L));
    }

    // ==================== Same API as a plain key-value store (AXPD-11068) ====================

    @Test
    @DisplayName("putIfAbsent with a timestamp returns only the existing value")
    void putIfAbsentWithTimestampReturnsOnlyTheValue() {
        when(delegate.putIfAbsent(any(), any())).thenReturn(ValueAndTimestamp.make("old", 50L));
        assertThat(runPython("""
                [store.putIfAbsent("key", "new", 100)]
                """)).containsExactly("old");
    }

    @Test
    @DisplayName("put(key, value) without a timestamp stores the value with the record timestamp")
    void putWithoutTimestampUsesRecordTimestamp() {
        runPython("""
                store.put("key", "value")
                []
                """);
        final ArgumentCaptor<ValueAndTimestamp<Object>> captor = ArgumentCaptor.captor();
        verify(delegate).put(any(), captor.capture());
        assertThat(captor.getValue().timestamp()).isEqualTo(RECORD_TIMESTAMP);
        assertThat(captor.getValue().value()).isNotNull();
    }

    @Test
    @DisplayName("putIfAbsent(key, value) without a timestamp uses the record timestamp and returns only the existing value")
    void putIfAbsentWithoutTimestampUsesRecordTimestamp() {
        when(delegate.putIfAbsent(any(), any())).thenReturn(ValueAndTimestamp.make("old", 50L));
        assertThat(runPython("""
                [store.putIfAbsent("key", "new")]
                """)).containsExactly("old");
        final ArgumentCaptor<ValueAndTimestamp<Object>> captor = ArgumentCaptor.captor();
        verify(delegate).putIfAbsent(any(), captor.capture());
        assertThat(captor.getValue().timestamp()).isEqualTo(RECORD_TIMESTAMP);
    }

    @Test
    @DisplayName("all, range, reverseRange, reverseAll and prefixScan return {key, value} entries without the timestamp")
    void iteratorsReturnEntriesWithoutTimestamp() {
        when(delegate.all()).thenReturn(entries("a", "b"));
        when(delegate.range("a", "b")).thenReturn(entries("a", "b"));
        when(delegate.reverseRange("a", "b")).thenReturn(entries("b", "a"));
        when(delegate.reverseAll()).thenReturn(entries("b", "a"));
        when(delegate.prefixScan(eq("a"), any())).thenReturn(entries("a", "ab"));
        assertThat(runPython("""
                def check(entries):
                    for e in entries:
                        assert e == {"key": e["key"], "value": "value-" + e["key"]}, e
                    return ",".join(e["key"] for e in entries)
                [check(list(store.all())), check(list(store.range("a", "b"))), check(list(store.reverseRange("a", "b"))),
                 check(list(store.reverseAll())), check(list(store.prefixScan("a")))]
                """)).containsExactly("a,b", "a,b", "b,a", "b,a", "a,ab");
    }

    @Test
    @DisplayName("the *WithTimestamp iterator methods return flat {key, value, timestamp} entries")
    void withTimestampIteratorsReturnFlatEntries() {
        when(delegate.all()).thenReturn(entries("a", "b"));
        when(delegate.range("a", "b")).thenReturn(entries("a", "b"));
        when(delegate.reverseRange("a", "b")).thenReturn(entries("b", "a"));
        when(delegate.reverseAll()).thenReturn(entries("b", "a"));
        when(delegate.prefixScan(eq("a"), any())).thenReturn(entries("a", "ab"));
        assertThat(runPython("""
                def check(entries):
                    for e in entries:
                        assert sorted(e.keys()) == ["key", "timestamp", "value"], e
                    return ",".join(e["key"] + ":" + e["value"] + "@" + str(e["timestamp"]) for e in entries)
                [check(list(store.allWithTimestamp())), check(list(store.rangeWithTimestamp("a", "b"))),
                 check(list(store.reverseRangeWithTimestamp("a", "b"))), check(list(store.reverseAllWithTimestamp())),
                 check(list(store.prefixScanWithTimestamp("a")))]
                """)).containsExactly(
                "a:value-a@100,b:value-b@101", "a:value-a@100,b:value-b@101", "b:value-b@100,a:value-a@101",
                "b:value-b@100,a:value-a@101", "a:value-a@100,ab:value-ab@101");
    }

    @Test
    @DisplayName("prefixScan and prefixScanWithTimestamp with a prefix that is not a string raise TypeError")
    void prefixScanWithNonStringPrefix() {
        assertThat(pythonErrorOf("store.prefixScan(42)")).startsWith("TypeError: ").contains("string prefix");
        assertThat(pythonErrorOf("store.prefixScanWithTimestamp(42)")).startsWith("TypeError: ").contains("string prefix");
        verify(delegate, never()).prefixScan(any(), any());
    }

    @Test
    @DisplayName("putAll with tuples and dicts writes all values with the record timestamp in one delegate putAll")
    void putAllWithTuplesAndDicts() {
        runPython("""
                store.putAll([("a", "1"), {"key": "b", "value": "2"}, ["c", "3"]])
                []
                """);
        assertThat(capturePutAll()).containsExactly(
                entry("a", "1", RECORD_TIMESTAMP), entry("b", "2", RECORD_TIMESTAMP), entry("c", "3", RECORD_TIMESTAMP));
        verify(delegate, never()).put(any(), any());
    }

    @Test
    @DisplayName("putAll writes a dict entry with its timestamp field")
    void putAllWithTimestampField() {
        runPython("""
                store.putAll([{"key": "a", "value": "1", "timestamp": 700}, {"key": "b", "value": "2", "timestamp": 800}])
                []
                """);
        assertThat(capturePutAll()).containsExactly(entry("a", "1", 700L), entry("b", "2", 800L));
    }

    @Test
    @DisplayName("putAll(other.allWithTimestamp()) copies the entries with their timestamps")
    void putAllCopiesWithTimestamps() {
        when(delegate.all()).thenReturn(entries("a", "b"));
        runPython("""
                store.putAll(store.allWithTimestamp())
                []
                """);
        assertThat(capturePutAll()).containsExactly(entry("a", "value-a", 100L), entry("b", "value-b", 101L));
    }

    @Test
    @DisplayName("putAll passes a None value to the delegate as null, which deletes the key")
    void putAllWithNoneValue() {
        runPython("""
                store.putAll([("a", None)])
                []
                """);
        assertThat(capturePutAll()).containsExactly(KeyValue.pair("a", null));
    }

    @Test
    @DisplayName("putAll with a bad entry raises TypeError naming its position, and writes nothing")
    void putAllWithBadEntry() {
        assertThat(pythonErrorOf("store.putAll([(\"a\", \"1\"), 42])"))
                .startsWith("TypeError: ").contains("putAll entry 1");
        verify(delegate, never()).putAll(anyList());
        verify(delegate, never()).put(any(), any());
    }

    @Test
    @DisplayName("putAll with a timestamp field that is not an int raises TypeError, and writes nothing")
    void putAllWithBadTimestampField() {
        assertThat(pythonErrorOf("store.putAll([{\"key\": \"a\", \"value\": \"1\", \"timestamp\": \"soon\"}])"))
                .startsWith("TypeError: ").contains("putAll entry 0 timestamp");
        verify(delegate, never()).putAll(anyList());
    }

    @Test
    @DisplayName("the timestamp error message names the Python type, never the value")
    void putAllTimestampErrorHasNoValue() {
        assertThat(pythonErrorOf("store.putAll([{\"key\": \"a\", \"value\": \"1\", \"timestamp\": \"secret\"}])"))
                .contains("got str").doesNotContain("secret");
    }

    @Test
    @DisplayName("putAll with a tuple of three raises ValueError: a timestamp per entry only comes from the dict field")
    void putAllWithThreeTupleIsAnError() {
        assertThat(pythonErrorOf("store.putAll([(\"a\", \"1\", 700)])"))
                .startsWith("ValueError: ").contains("putAll entry 0");
        verify(delegate, never()).putAll(anyList());
    }
}
