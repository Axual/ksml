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

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.python.PythonContext;
import io.axual.ksml.python.PythonContextConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KeyValueStoreProxyTest {

    @Mock
    private KeyValueStore<Object, Object> delegate;

    @Mock
    private KeyValueIterator<Object, Object> iterator;

    private KeyValueStoreProxy proxy() {
        return new KeyValueStoreProxy(delegate);
    }

    @Test
    @DisplayName("get delegates to the store and returns the value as a Python value")
    void getConvertsKeyAndReturnsResult() {
        when(delegate.get("key")).thenReturn("value");
        assertThat(proxy().get("key")).isInstanceOfSatisfying(Value.class,
                value -> assertThat(value.asString()).isEqualTo("value"));
        verify(delegate).get("key");
    }

    @Test
    @DisplayName("put forwards the key and value to the delegate store")
    void putForwardsToDelegate() {
        proxy().put("key", "value");
        verify(delegate).put("key", "value");
    }

    @Test
    @DisplayName("delete forwards to the delegate and returns the removed value as a Python value")
    void deleteForwardsToDelegate() {
        when(delegate.delete("key")).thenReturn("old");
        assertThat(proxy().delete("key")).isInstanceOfSatisfying(Value.class,
                value -> assertThat(value.asString()).isEqualTo("old"));
        verify(delegate).delete("key");
    }

    @Test
    @DisplayName("putIfAbsent forwards the key and value to the delegate store")
    void putIfAbsentForwardsToDelegate() {
        when(delegate.putIfAbsent("key", "value")).thenReturn(null);
        proxy().putIfAbsent("key", "value");
        verify(delegate).putIfAbsent("key", "value");
    }

    @Test
    @DisplayName("approximateNumEntries returns the delegate count as a Python value")
    void approximateNumEntriesIsConverted() {
        when(delegate.approximateNumEntries()).thenReturn(5L);
        assertThat(proxy().approximateNumEntries()).isInstanceOfSatisfying(Value.class,
                value -> assertThat(value.asLong()).isEqualTo(5L));
    }

    @Test
    @DisplayName("all wraps the delegate iterator in a StoreIteratorProxy")
    void allWrapsIteratorInProxy() {
        when(delegate.all()).thenReturn(iterator);
        assertThat(proxy().all()).isInstanceOf(StoreIteratorProxy.class);
    }

    @Test
    @DisplayName("name delegates to the wrapped store")
    void nameDelegates() {
        when(delegate.name()).thenReturn("myStore");
        assertThat(proxy().name()).isEqualTo("myStore");
    }

    @Test
    @DisplayName("persistent and isOpen delegate to the wrapped store")
    void persistentAndIsOpenDelegate() {
        when(delegate.persistent()).thenReturn(true);
        when(delegate.isOpen()).thenReturn(true);
        assertThat(proxy().persistent()).isTrue();
        assertThat(proxy().isOpen()).isTrue();
    }

    @Test
    @DisplayName("flush and close delegate to the wrapped store")
    void flushAndCloseDelegate() {
        final var proxy = proxy();
        proxy.flush();
        proxy.close();
        verify(delegate).flush();
        verify(delegate).close();
    }

    @Test
    @DisplayName("init forwards the context and root store to the wrapped store")
    void initDelegates() {
        final var context = mock(StateStoreContext.class);
        final var root = mock(StateStore.class);
        proxy().init(context, root);
        verify(delegate).init(context, root);
    }

    @Test
    @DisplayName("toString includes the proxy type and the store name")
    void toStringIncludesName() {
        when(delegate.name()).thenReturn("myStore");
        assertThat(proxy()).asString().contains("KeyValueStoreProxy").contains("myStore");
    }

    @Test
    @DisplayName("a value read back from the store survives copy.deepcopy()")
    void deepcopyOnAStateStoreReadResult() {
        var nested = new DataStruct();
        nested.put("city", new DataString("Amsterdam"));
        when(delegate.get("sensor1")).thenReturn(nested);

        try (var pythonContext = new PythonContext(PythonContextConfig.builder().build())) {
            pythonContext.context().getBindings("python").putMember("store", proxy());
            var isDict = pythonContext.context().eval("python", "type(store.get('sensor1')) is dict");
            assertThat(isDict.asBoolean()).isTrue();

            var deepcopyResult = pythonContext.context().eval("python", """
                    import copy
                    value = store.get('sensor1')
                    copy.deepcopy(value)
                    'OK'
                    """);
            assertThat(deepcopyResult.asString()).isEqualTo("OK");
        }
    }

    // ==================== Methods called from Python ====================

    private static FakeKeyValueIterator<Object> entries(String... keys) {
        final var result = new ArrayList<KeyValue<Object, Object>>();
        for (final var key : keys) result.add(KeyValue.pair(key, "value-" + key));
        return new FakeKeyValueIterator<>(result);
    }

    /** Runs Python code with {@code store} bound to the proxy, and returns the result as a list of strings. */
    private List<String> runPython(String code) {
        try (var pythonContext = new PythonContext(PythonContextConfig.builder().build())) {
            pythonContext.context().getBindings("python").putMember("store", proxy());
            final var result = pythonContext.context().eval("python", code);
            final var strings = new ArrayList<String>();
            for (long i = 0; i < result.getArraySize(); i++) strings.add(result.getArrayElement(i).asString());
            return strings;
        }
    }

    /** Runs Python code with {@code store} bound to the proxy, and expects it to raise an error. */
    private PolyglotException runPythonExpectingError(String code) {
        try (var pythonContext = new PythonContext(PythonContextConfig.builder().build())) {
            pythonContext.context().getBindings("python").putMember("store", proxy());
            return (PolyglotException) catchThrowable(() -> pythonContext.context().eval("python", code));
        }
    }

    @Test
    @DisplayName("range(from, to) returns {key, value} entries, and closes the Kafka iterator")
    void rangeFromPython() {
        final var kafkaIterator = entries("a", "b", "c");
        when(delegate.range("a", "c")).thenReturn(kafkaIterator);
        assertThat(runPython("""
                [e["key"] + "=" + e["value"] for e in store.range("a", "c")]
                """)).containsExactly("a=value-a", "b=value-b", "c=value-c");
        assertThat(kafkaIterator.closeCount).isEqualTo(1);
    }

    @Test
    @DisplayName("range with None bounds passes open bounds to the delegate")
    void rangeWithOpenBoundsFromPython() {
        when(delegate.range(null, "b")).thenReturn(entries("a", "b"));
        when(delegate.range("b", null)).thenReturn(entries("b", "c"));
        assertThat(runPython("""
                [e["key"] for e in store.range(None, "b")] + [e["key"] for e in store.range("b", None)]
                """)).containsExactly("a", "b", "b", "c");
    }

    @Test
    @DisplayName("reverseRange(from, to) returns the reverse range, and closes the Kafka iterator")
    void reverseRangeFromPython() {
        final var kafkaIterator = entries("c", "b", "a");
        when(delegate.reverseRange("a", "c")).thenReturn(kafkaIterator);
        assertThat(runPython("""
                [e["key"] for e in store.reverseRange("a", "c")]
                """)).containsExactly("c", "b", "a");
        assertThat(kafkaIterator.closeCount).isEqualTo(1);
    }

    @Test
    @DisplayName("reverseAll() returns all entries in reverse order, and closes the Kafka iterator")
    void reverseAllFromPython() {
        final var kafkaIterator = entries("c", "b", "a");
        when(delegate.reverseAll()).thenReturn(kafkaIterator);
        assertThat(runPython("""
                [e["key"] for e in store.reverseAll()]
                """)).containsExactly("c", "b", "a");
        assertThat(kafkaIterator.closeCount).isEqualTo(1);
    }

    @Test
    @DisplayName("prefixScan(prefix) passes the prefix and a StringSerializer, and closes the Kafka iterator")
    void prefixScanFromPython() {
        final var kafkaIterator = entries("sensor1", "sensor2");
        when(delegate.prefixScan(eq("se"), any(StringSerializer.class))).thenReturn(kafkaIterator);
        assertThat(runPython("""
                [e["key"] for e in store.prefixScan("se")]
                """)).containsExactly("sensor1", "sensor2");
        assertThat(kafkaIterator.closeCount).isEqualTo(1);
    }

    @Test
    @DisplayName("prefixScan(\"\") reads all(), as Kafka's in-memory and caching stores fail on an empty prefix")
    void prefixScanWithEmptyPrefix() {
        final var kafkaIterator = entries("a", "b");
        when(delegate.all()).thenReturn(kafkaIterator);
        assertThat(runPython("""
                [e["key"] for e in store.prefixScan("")]
                """)).containsExactly("a", "b");
        verify(delegate, never()).prefixScan(any(), any());
        assertThat(kafkaIterator.closeCount).isEqualTo(1);
    }

    @Test
    @DisplayName("putAll with (key, value) tuples writes all entries with one delegate putAll")
    void putAllWithTuples() {
        runPython("""
                store.putAll([("a", "1"), ("b", "2")])
                []
                """);
        verify(delegate).putAll(List.of(KeyValue.pair("a", "1"), KeyValue.pair("b", "2")));
        verify(delegate, never()).put(any(), any());
    }

    @ParameterizedTest(name = "putAll with {0} writes all entries")
    @CsvSource(delimiter = '|', textBlock = """
            [key, value] lists          | [["a", "1"], ["b", "2"]]
            {key, value} dicts          | [{"key": "a", "value": "1"}, {"key": "b", "value": "2"}]
            tuples and dicts mixed      | [("a", "1"), {"key": "b", "value": "2"}]
            a Python iterator           | iter([("a", "1"), ("b", "2")])
            """)
    void putAllWithEntryShapes(String shape, String entries) {
        runPython("store.putAll(" + entries + ")\n[]\n");
        verify(delegate).putAll(List.of(KeyValue.pair("a", "1"), KeyValue.pair("b", "2")));
    }

    @Test
    @DisplayName("putAll accepts a store iterator, so store.putAll(other.all()) copies a store")
    void putAllWithStoreIterator() {
        when(delegate.all()).thenReturn(entries("a", "b"));
        runPython("""
                store.putAll(store.all())
                []
                """);
        verify(delegate).putAll(List.of(KeyValue.pair("a", "value-a"), KeyValue.pair("b", "value-b")));
    }

    @Test
    @DisplayName("putAll passes a None value to the delegate, which deletes the key")
    void putAllWithNoneValue() {
        runPython("""
                store.putAll([("a", None)])
                []
                """);
        verify(delegate).putAll(Arrays.asList(KeyValue.pair("a", null)));
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

    @Test
    @DisplayName("putAll with an entry that is not a pair or a dict raises TypeError naming its position, and writes nothing")
    void putAllWithBadEntry() {
        assertThat(pythonErrorOf("store.putAll([(\"a\", \"1\"), 42])"))
                .startsWith("TypeError: ").contains("putAll entry 1");
        verify(delegate, never()).putAll(anyList());
        verify(delegate, never()).put(any(), any());
    }

    @Test
    @DisplayName("putAll with a string entry raises TypeError instead of reading it as a pair")
    void putAllWithStringEntry() {
        assertThat(pythonErrorOf("store.putAll([\"ab\"])")).startsWith("TypeError: ").contains("putAll entry 0");
        verify(delegate, never()).putAll(anyList());
    }

    @Test
    @DisplayName("putAll with something that is not a list or an iterator raises TypeError")
    void putAllWithNonIterable() {
        assertThat(pythonErrorOf("store.putAll(42)")).startsWith("TypeError: ").contains("list or an iterator");
        verify(delegate, never()).putAll(anyList());
    }

    @Test
    @DisplayName("putAll with a tuple of three or a dict without a value raises ValueError naming its position")
    void putAllWithIncompleteEntries() {
        assertThat(pythonErrorOf("store.putAll([(\"a\", \"1\", \"2\")])"))
                .startsWith("ValueError: ").contains("putAll entry 0");
        assertThat(pythonErrorOf("store.putAll([{\"key\": \"a\"}])"))
                .startsWith("ValueError: ").contains("putAll entry 0");
        verify(delegate, never()).putAll(anyList());
    }

    @Test
    @DisplayName("prefixScan with a prefix that is not a string raises TypeError")
    void prefixScanWithNonStringPrefix() {
        assertThat(pythonErrorOf("store.prefixScan(42)")).startsWith("TypeError: ").contains("string prefix");
        verify(delegate, never()).prefixScan(any(), any());
    }

    @Test
    @DisplayName("an uncaught caller error reaches Java as a Python TypeError, not as a host exception")
    void uncaughtCallerErrorIsAPythonError() {
        final var error = runPythonExpectingError("""
                store.putAll([42])
                """);
        assertThat(error.isGuestException()).isTrue();
        assertThat(error.isHostException()).isFalse();
        assertThat(error.getMessage()).startsWith("TypeError: ").contains("putAll entry 0");
    }

    @Test
    @DisplayName("putAll with a dict raises a TypeError that points to d.items()")
    void putAllWithDictRaisesTypeError() {
        final var error = runPythonExpectingError("""
                store.putAll({"a": "1"})
                """);
        assertThat(error.getMessage()).startsWith("TypeError: ").contains("putAll(d.items())");
        verify(delegate, never()).putAll(any());
    }

    @Test
    @DisplayName("a caller error from Java, without a Python context, throws a DataException with the message")
    void callerErrorWithoutPythonContext() {
        assertThatThrownBy(() -> proxy().prefixScan(42))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("string prefix");
        verify(delegate, never()).prefixScan(any(), any());
    }

    @Test
    @DisplayName("a store error is not a caller error: it stays a Java exception, which except Exception does not catch")
    void storeErrorStaysAJavaException() {
        doThrow(new InvalidStateStoreException("store is closed")).when(delegate).putAll(anyList());
        assertThat(runPython("""
                def run():
                    try:
                        try:
                            store.putAll([("a", "1")])
                        except Exception:
                            return "caught by except Exception"
                    except BaseException as e:
                        return "only except BaseException: " + str(e)
                [run()]
                """)).singleElement().asString()
                .startsWith("only except BaseException: ").contains("store is closed");
    }
}
