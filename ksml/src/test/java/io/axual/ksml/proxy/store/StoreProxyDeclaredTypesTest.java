package io.axual.ksml.proxy.store;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2026 Axual B.V.
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

import io.axual.ksml.data.object.DataBytes;
import io.axual.ksml.data.object.DataDouble;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.python.PythonContext;
import io.axual.ksml.python.PythonContextConfig;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The store proxies convert Python keys and values to the store's declared simple types (AXPD-11068): a Python int
 * becomes a long on a long store whatever its size. A value of another kind raises TypeError, a value out of range
 * raises ValueError; neither writes anything, and the message names the types but not the value.
 */
@SuppressWarnings("unchecked")
class StoreProxyDeclaredTypesTest {
    private static final DataType LONG = DataLong.DATATYPE;
    private static PythonContext pythonContext;

    @BeforeAll
    static void setUpContext() {
        pythonContext = new PythonContext(PythonContextConfig.builder().build());
    }

    @AfterAll
    static void tearDownContext() {
        pythonContext.close();
    }

    /** Runs Python code with {@code store} bound to the proxy; returns "ok", or "ErrorType: message". */
    private static String run(Object proxy, String call) {
        return StoreProxyTestUtil.pythonErrorOf(pythonContext.context(), proxy, call);
    }

    private static void assertLong(Object object, long expected) {
        assertThat(object).isInstanceOfSatisfying(DataLong.class, value -> assertThat(value.value()).isEqualTo(expected));
    }

    @Test
    @DisplayName("Key-value store: put, putIfAbsent, putAll, get and delete convert Python ints to the declared long type")
    void keyValueStoreConvertsInts() {
        final KeyValueStore<Object, Object> delegate = mock(KeyValueStore.class);
        final var proxy = new KeyValueStoreProxy(delegate, LONG, LONG);

        assertThat(run(proxy, "store.put(1, 5)")).isEqualTo("ok");
        final var key = ArgumentCaptor.forClass(Object.class);
        final var value = ArgumentCaptor.forClass(Object.class);
        verify(delegate).put(key.capture(), value.capture());
        assertLong(key.getValue(), 1);
        assertLong(value.getValue(), 5);

        assertThat(run(proxy, "store.putIfAbsent(2, 2 ** 40)")).isEqualTo("ok");
        verify(delegate).putIfAbsent(key.capture(), value.capture());
        assertLong(value.getValue(), 1L << 40);

        assertThat(run(proxy, "store.putAll([(3, 5), {'key': 4, 'value': 6}])")).isEqualTo("ok");
        final ArgumentCaptor<List<KeyValue<Object, Object>>> entries = ArgumentCaptor.captor();
        verify(delegate).putAll(entries.capture());
        assertLong(entries.getValue().get(0).key, 3);
        assertLong(entries.getValue().get(0).value, 5);
        assertLong(entries.getValue().get(1).value, 6);

        assertThat(run(proxy, "store.get(7)")).isEqualTo("ok");
        verify(delegate).get(key.capture());
        assertLong(key.getValue(), 7);

        assertThat(run(proxy, "store.delete(8)")).isEqualTo("ok");
        verify(delegate).delete(key.capture());
        assertLong(key.getValue(), 8);
    }

    @Test
    @DisplayName("Key-value store: None still deletes the key, and is not converted")
    void keyValueStoreNoneValue() {
        final KeyValueStore<Object, Object> delegate = mock(KeyValueStore.class);
        assertThat(run(new KeyValueStoreProxy(delegate, LONG, LONG), "store.put(1, None)")).isEqualTo("ok");
        verify(delegate).put(any(), isNull());
    }

    @Test
    @DisplayName("A None key in put, putIfAbsent or putAll raises ValueError, as Kafka rejects a null key, and writes nothing")
    void noneKeyInWrites() {
        final KeyValueStore<Object, Object> kvStore = mock(KeyValueStore.class);
        when(kvStore.name()).thenReturn("counts");
        final var kv = new KeyValueStoreProxy(kvStore, LONG, LONG);
        assertThat(run(kv, "store.put(None, 5)")).isEqualTo("ValueError: Store counts: key must not be None");
        assertThat(run(kv, "store.putIfAbsent(None, 5)")).isEqualTo("ValueError: Store counts: key must not be None");
        assertThat(run(kv, "store.putAll([(1, 5), (None, 6)])"))
                .isEqualTo("ValueError: Store counts: putAll entry 1 key must not be None");
        assertThat(run(kv, "store.putAll([{'key': None, 'value': 6}])"))
                .isEqualTo("ValueError: Store counts: putAll entry 0 key must not be None");
        verify(kvStore, never()).put(any(), any());
        verify(kvStore, never()).putIfAbsent(any(), any());
        verify(kvStore, never()).putAll(anyList());

        final TimestampedKeyValueStore<Object, Object> tsStore = mock(TimestampedKeyValueStore.class);
        when(tsStore.name()).thenReturn("counts");
        final var ts = new TimestampedKeyValueStoreProxy(tsStore, LONG, LONG, () -> 1000L);
        assertThat(run(ts, "store.put(None, 5)")).isEqualTo("ValueError: Store counts: key must not be None");
        assertThat(run(ts, "store.put(None, None)")).isEqualTo("ValueError: Store counts: key must not be None");
        assertThat(run(ts, "store.putIfAbsent(None, 5, 2000)")).isEqualTo("ValueError: Store counts: key must not be None");
        assertThat(run(ts, "store.putAll([(None, 6)])")).isEqualTo("ValueError: Store counts: putAll entry 0 key must not be None");
        verify(tsStore, never()).put(any(), any());
        verify(tsStore, never()).putIfAbsent(any(), any());
        verify(tsStore, never()).putAll(anyList());

        final WindowStore<Object, Object> windowStore = mock(WindowStore.class);
        when(windowStore.name()).thenReturn("counts");
        assertThat(run(new WindowStoreProxy(windowStore, LONG, LONG), "store.put(None, 5, 0)"))
                .isEqualTo("ValueError: Store counts: key must not be None");
        verify(windowStore, never()).put(any(), any(), anyLong());

        final TimestampedWindowStore<Object, Object> tsWindowStore = mock(TimestampedWindowStore.class);
        when(tsWindowStore.name()).thenReturn("counts");
        final var tsWindow = new TimestampedWindowStoreProxy(tsWindowStore, LONG, LONG, () -> 1000L);
        assertThat(run(tsWindow, "store.put(None, 5, 0)")).isEqualTo("ValueError: Store counts: key must not be None");
        assertThat(run(tsWindow, "store.put(None, None, 0)")).isEqualTo("ValueError: Store counts: key must not be None");
        verify(tsWindowStore, never()).put(any(), any(), anyLong());

        final VersionedKeyValueStore<Object, Object> versionedStore = mock(VersionedKeyValueStore.class);
        when(versionedStore.name()).thenReturn("counts");
        assertThat(run(new VersionedKeyValueStoreProxy(versionedStore, LONG, LONG), "store.put(None, 5, 1000)"))
                .isEqualTo("ValueError: Store counts: key must not be None");
        verify(versionedStore, never()).put(any(), any(), anyLong());
    }

    @Test
    @DisplayName("A None key in a single-key read or delete raises ValueError, as Kafka rejects a null key, and calls no store")
    void noneKeyInSingleKeyReadsAndDeletes() {
        final var error = "ValueError: Store counts: key must not be None";
        final KeyValueStore<Object, Object> kvStore = mock(KeyValueStore.class);
        when(kvStore.name()).thenReturn("counts");
        final var kv = new KeyValueStoreProxy(kvStore, LONG, LONG);
        assertThat(run(kv, "store.get(None)")).isEqualTo(error);
        assertThat(run(kv, "store.delete(None)")).isEqualTo(error);
        verify(kvStore, never()).get(any());
        verify(kvStore, never()).delete(any());

        final TimestampedKeyValueStore<Object, Object> tsStore = mock(TimestampedKeyValueStore.class);
        when(tsStore.name()).thenReturn("counts");
        final var ts = new TimestampedKeyValueStoreProxy(tsStore, LONG, LONG, () -> 1000L);
        assertThat(run(ts, "store.get(None)")).isEqualTo(error);
        assertThat(run(ts, "store.getWithTimestamp(None)")).isEqualTo(error);
        assertThat(run(ts, "store.delete(None)")).isEqualTo(error);
        verify(tsStore, never()).get(any());
        verify(tsStore, never()).delete(any());

        final VersionedKeyValueStore<Object, Object> versionedStore = mock(VersionedKeyValueStore.class);
        when(versionedStore.name()).thenReturn("counts");
        final var versioned = new VersionedKeyValueStoreProxy(versionedStore, LONG, LONG);
        assertThat(run(versioned, "store.get(None)")).isEqualTo(error);
        assertThat(run(versioned, "store.get(None, 1000)")).isEqualTo(error);
        assertThat(run(versioned, "store.delete(None, 1000)")).isEqualTo(error);
        verify(versionedStore, never()).get(any());
        verify(versionedStore, never()).get(any(), anyLong());
        verify(versionedStore, never()).delete(any(), anyLong());

        final WindowStore<Object, Object> windowStore = mock(WindowStore.class);
        when(windowStore.name()).thenReturn("counts");
        final var window = new WindowStoreProxy(windowStore, LONG, LONG);
        assertThat(run(window, "store.fetch(None, 0)")).isEqualTo(error);
        assertThat(run(window, "store.fetch(None, 0, 1000)")).isEqualTo(error);
        verify(windowStore, never()).fetch(any(), anyLong());
        verify(windowStore, never()).fetch(any(), anyLong(), anyLong());

        final TimestampedWindowStore<Object, Object> tsWindowStore = mock(TimestampedWindowStore.class);
        when(tsWindowStore.name()).thenReturn("counts");
        final var tsWindow = new TimestampedWindowStoreProxy(tsWindowStore, LONG, LONG, () -> 1000L);
        assertThat(run(tsWindow, "store.fetchWithTimestamp(None, 0)")).isEqualTo(error);
        verify(tsWindowStore, never()).fetch(any(), anyLong());

        final SessionStore<Object, Object> sessionStore = mock(SessionStore.class);
        when(sessionStore.name()).thenReturn("counts");
        final var session = new SessionStoreProxy(sessionStore, LONG, LONG);
        assertThat(run(session, "store.fetch(None)")).isEqualTo(error);
        assertThat(run(session, "store.findSessions(None, 0, 1000)")).isEqualTo(error);
        verify(sessionStore, never()).fetch(any());
        verify(sessionStore, never()).findSessions(any(), anyLong(), anyLong());
    }

    @Test
    @DisplayName("Timestamped stores: None deletes with the key converted to the declared type, also without a record")
    void timestampedStoreNoneValueConvertsKey() {
        final TimestampedKeyValueStore<Object, Object> kvStore = mock(TimestampedKeyValueStore.class);
        final var kv = new TimestampedKeyValueStoreProxy(kvStore, LONG, LONG, ProxyUtil::noRecordTimestamp);
        assertThat(run(kv, "store.put(5, None)")).isEqualTo("ok");
        final var key = ArgumentCaptor.forClass(Object.class);
        verify(kvStore).put(key.capture(), isNull());
        assertLong(key.getValue(), 5);
        assertThat(run(kv, "store.putAll([(6, None)])")).isEqualTo("ok");
        final ArgumentCaptor<List<KeyValue<Object, ValueAndTimestamp<Object>>>> entries = ArgumentCaptor.captor();
        verify(kvStore).putAll(entries.capture());
        assertLong(entries.getValue().get(0).key, 6);
        assertThat(entries.getValue().get(0).value).isNull();

        final TimestampedWindowStore<Object, Object> windowStore = mock(TimestampedWindowStore.class);
        final var window = new TimestampedWindowStoreProxy(windowStore, LONG, LONG, ProxyUtil::noRecordTimestamp);
        assertThat(run(window, "store.put(7, None, 0)")).isEqualTo("ok");
        verify(windowStore).put(key.capture(), isNull(), anyLong());
        assertLong(key.getValue(), 7);
    }

    @Test
    @DisplayName("Key-value store: a float, a str or a bool on a long store raises TypeError naming the types, and writes nothing")
    void keyValueStoreRejectsOtherKinds() {
        final KeyValueStore<Object, Object> delegate = mock(KeyValueStore.class);
        when(delegate.name()).thenReturn("counts");
        final var proxy = new KeyValueStoreProxy(delegate, LONG, LONG);

        assertThat(run(proxy, "store.put(1, 1.5)")).isEqualTo("TypeError: Store counts: value must be long, got float");
        assertThat(run(proxy, "store.put(1, 5.0)")).isEqualTo("TypeError: Store counts: value must be long, got float");
        assertThat(run(proxy, "store.put(1, '5')")).isEqualTo("TypeError: Store counts: value must be long, got str");
        assertThat(run(proxy, "store.put(1, True)")).isEqualTo("TypeError: Store counts: value must be long, got bool");
        assertThat(run(proxy, "store.put(1.0, 5)")).isEqualTo("TypeError: Store counts: key must be long, got float");
        // A putAll error names the position of the entry
        assertThat(run(proxy, "store.putAll([(1, 2), (2, 1.5)])"))
                .isEqualTo("TypeError: Store counts: putAll entry 1 value must be long, got float");
        verify(delegate, never()).put(any(), any());
        verify(delegate, never()).putAll(anyList());
    }

    @Test
    @DisplayName("Key-value store: an int out of range raises ValueError without the value in the message, and writes nothing")
    void keyValueStoreRejectsOutOfRange() {
        final KeyValueStore<Object, Object> delegate = mock(KeyValueStore.class);
        when(delegate.name()).thenReturn("counts");

        assertThat(run(new KeyValueStoreProxy(delegate, LONG, LONG), "store.put(1, 10 ** 30)"))
                .isEqualTo("ValueError: Store counts: value int is out of range for long");
        assertThat(run(new KeyValueStoreProxy(delegate, LONG, LONG), "store.get(10 ** 30)"))
                .isEqualTo("ValueError: Store counts: key int is out of range for long");
        assertThat(run(new KeyValueStoreProxy(delegate, DataInteger.DATATYPE, DataInteger.DATATYPE), "store.put(1, 2 ** 40)"))
                .isEqualTo("ValueError: Store counts: value int is out of range for integer");
        // Python's own float(10 ** 400) fails too; the store must not get Infinity
        assertThat(run(new KeyValueStoreProxy(delegate, LONG, DataDouble.DATATYPE), "store.put(1, 10 ** 400)"))
                .isEqualTo("ValueError: Store counts: value int is out of range for double");
        // The message must not contain the value, which may be user data
        assertThat(run(new KeyValueStoreProxy(delegate, LONG, LONG), "store.putAll([(10 ** 30, 1)])"))
                .isEqualTo("ValueError: Store counts: putAll entry 0 key int is out of range for long");
        assertThat(run(new KeyValueStoreProxy(delegate, LONG, LONG), "store.putAll([{'key': 1, 'value': 10 ** 30}])"))
                .isEqualTo("ValueError: Store counts: putAll entry 0 value int is out of range for long");
        verify(delegate, never()).put(any(), any());
        verify(delegate, never()).putAll(anyList());
        verify(delegate, never()).get(any());
    }

    @Test
    @DisplayName("Key-value store: an int on a double store becomes a double; a non-string on a string store raises TypeError")
    void keyValueStoreOtherSimpleTypes() {
        final KeyValueStore<Object, Object> delegate = mock(KeyValueStore.class);
        when(delegate.name()).thenReturn("names");

        assertThat(run(new KeyValueStoreProxy(delegate, LONG, DataDouble.DATATYPE), "store.put(1, 5)")).isEqualTo("ok");
        final var value = ArgumentCaptor.forClass(Object.class);
        verify(delegate).put(any(), value.capture());
        assertThat(value.getValue()).isInstanceOfSatisfying(DataDouble.class, d -> assertThat(d.value()).isEqualTo(5.0));

        final var stringProxy = new KeyValueStoreProxy(delegate, DataString.DATATYPE, DataString.DATATYPE);
        assertThat(run(stringProxy, "store.put('a', 5)")).isEqualTo("TypeError: Store names: value must be string, got int");
        assertThat(run(stringProxy, "store.put('a', {'b': 1})")).isEqualTo("TypeError: Store names: value must be string, got dict");
    }

    @Test
    @DisplayName("Bytes store: bytes, bytearray, and lists or tuples of ints from -128 to 255, work; other values raise a Python error")
    void bytesStoreValues() {
        final KeyValueStore<Object, Object> delegate = mock(KeyValueStore.class);
        when(delegate.name()).thenReturn("blobs");
        final var proxy = new KeyValueStoreProxy(delegate, LONG, DataBytes.DATATYPE);

        assertThat(run(proxy, "store.put(1, b'ab')")).isEqualTo("ok");
        assertThat(run(proxy, "store.put(1, [-128, 255])")).isEqualTo("ok");
        assertThat(run(proxy, "store.put(1, (1, 2))")).isEqualTo("ok");
        assertThat(run(proxy, "store.put(1, bytearray(b'ab'))")).isEqualTo("ok");
        verify(delegate, times(4)).put(any(), any());

        final var outOfRange = "ValueError: Store blobs: value has an int out of range for bytes (-128 to 255)";
        assertThat(run(proxy, "store.put(1, [300])")).isEqualTo(outOfRange);
        assertThat(run(proxy, "store.put(1, [-129])")).isEqualTo(outOfRange);
        assertThat(run(proxy, "store.put(1, [2 ** 40])")).isEqualTo(outOfRange);
        assertThat(run(proxy, "store.put(1, ['a'])")).isEqualTo("TypeError: Store blobs: value must be bytes, got list");
        assertThat(run(proxy, "store.put(1, {'a': 1})")).isEqualTo("TypeError: Store blobs: value must be bytes, got dict");
        // A value that is not bytes, a list or a tuple is not read element by element, so it is a TypeError
        assertThat(run(proxy, "store.put(1, 5)")).isEqualTo("TypeError: Store blobs: value must be bytes, got int");
        assertThat(run(proxy, "store.put(1, 1.5)")).isEqualTo("TypeError: Store blobs: value must be bytes, got float");
        assertThat(run(proxy, "store.put(1, True)")).isEqualTo("TypeError: Store blobs: value must be bytes, got bool");
        assertThat(run(proxy, "store.put(1, 10 ** 30)")).isEqualTo("TypeError: Store blobs: value must be bytes, got int");
        assertThat(run(proxy, "store.put(1, {1, 2})")).startsWith("TypeError: Store blobs: value must be bytes, got ");
        // Only int elements: a float or a bool element is a TypeError, as on an integer store
        assertThat(run(proxy, "store.put(1, [1.0])")).isEqualTo("TypeError: Store blobs: value must be bytes, got list");
        assertThat(run(proxy, "store.put(1, (1, True))")).isEqualTo("TypeError: Store blobs: value must be bytes, got tuple");
        assertThat(run(proxy, "store.putAll([(1, [300])])"))
                .isEqualTo("ValueError: Store blobs: putAll entry 0 value has an int out of range for bytes (-128 to 255)");
        verify(delegate, times(4)).put(any(), any());
        verify(delegate, never()).putAll(anyList());
    }

    @Test
    @DisplayName("prefixScan raises TypeError on a store whose declared key type is not string")
    @SuppressWarnings({"unchecked", "rawtypes"})
    void prefixScanNeedsStringKeys() {
        final KeyValueStore<Object, Object> delegate = mock(KeyValueStore.class);
        when(delegate.prefixScan(any(), any())).thenAnswer(invocation -> new FakeKeyValueIterator<>(List.of()));
        final var expected = "TypeError: prefixScan needs a store with string keys, got keyType long";
        assertThat(run(new KeyValueStoreProxy(delegate, LONG, LONG), "store.prefixScan('a')")).isEqualTo(expected);
        assertThat(run(new KeyValueStoreProxy(delegate, DataString.DATATYPE, LONG), "store.prefixScan('a')")).isEqualTo("ok");
        assertThat(run(new KeyValueStoreProxy(delegate, null, null), "store.prefixScan('a')")).isEqualTo("ok");

        final TimestampedKeyValueStore<Object, Object> timestamped = mock(TimestampedKeyValueStore.class);
        when(timestamped.prefixScan(any(), any())).thenAnswer(invocation -> (KeyValueIterator) new FakeKeyValueIterator<>(List.of()));
        final var timestampedProxy = new TimestampedKeyValueStoreProxy(timestamped, LONG, LONG, () -> 0L);
        assertThat(run(timestampedProxy, "store.prefixScan('a')")).isEqualTo(expected);
        assertThat(run(timestampedProxy, "store.prefixScanWithTimestamp('a')")).isEqualTo(expected);
        verify(timestamped, never()).prefixScan(any(), any());
    }

    @Test
    @DisplayName("Key-value store: without a declared type, or with a type that is not simple, values are not converted")
    void keyValueStoreWithoutSimpleTypeIsUnchanged() {
        final KeyValueStore<Object, Object> delegate = mock(KeyValueStore.class);
        assertThat(run(new KeyValueStoreProxy(delegate, null, null), "store.put(1, 5)")).isEqualTo("ok");
        assertThat(run(new KeyValueStoreProxy(delegate, DataType.UNKNOWN, DataType.UNKNOWN), "store.put(1, 5)")).isEqualTo("ok");
        final var value = ArgumentCaptor.forClass(Object.class);
        verify(delegate, times(2)).put(any(), value.capture());
        assertThat(value.getAllValues()).allSatisfy(v -> assertThat(v).isEqualTo(5));
    }

    @Test
    @DisplayName("Timestamped key-value store: put, putAll and get convert Python ints to the declared long type")
    void timestampedKeyValueStoreConvertsInts() {
        final TimestampedKeyValueStore<Object, Object> delegate = mock(TimestampedKeyValueStore.class);
        when(delegate.name()).thenReturn("counts");
        final var proxy = new TimestampedKeyValueStoreProxy(delegate, LONG, LONG, () -> 1000L);

        assertThat(run(proxy, "store.put(1, 5)")).isEqualTo("ok");
        final var key = ArgumentCaptor.forClass(Object.class);
        final ArgumentCaptor<ValueAndTimestamp<Object>> value = ArgumentCaptor.captor();
        verify(delegate).put(key.capture(), value.capture());
        assertLong(key.getValue(), 1);
        assertLong(value.getValue().value(), 5);

        assertThat(run(proxy, "store.putAll([(3, 5)])")).isEqualTo("ok");
        final ArgumentCaptor<List<KeyValue<Object, ValueAndTimestamp<Object>>>> entries = ArgumentCaptor.captor();
        verify(delegate).putAll(entries.capture());
        assertLong(entries.getValue().get(0).key, 3);
        assertLong(entries.getValue().get(0).value.value(), 5);

        assertThat(run(proxy, "store.get(7)")).isEqualTo("ok");
        verify(delegate).get(key.capture());
        assertLong(key.getValue(), 7);

        assertThat(run(proxy, "store.put(1, 10 ** 30)")).isEqualTo("ValueError: Store counts: value int is out of range for long");
        assertThat(run(proxy, "store.put(1, 1.5, 2000)")).isEqualTo("TypeError: Store counts: value must be long, got float");
        assertThat(run(proxy, "store.putAll([(1, 2), {'key': 2.5, 'value': 3}])"))
                .isEqualTo("TypeError: Store counts: putAll entry 1 key must be long, got float");
    }

    @Test
    @DisplayName("Versioned key-value store: put and get convert Python ints to the declared long type")
    void versionedKeyValueStoreConvertsInts() {
        final VersionedKeyValueStore<Object, Object> delegate = mock(VersionedKeyValueStore.class);
        when(delegate.name()).thenReturn("counts");
        final var proxy = new VersionedKeyValueStoreProxy(delegate, LONG, LONG);

        assertThat(run(proxy, "store.put(1, 5, 1000)")).isEqualTo("ok");
        final var key = ArgumentCaptor.forClass(Object.class);
        final var value = ArgumentCaptor.forClass(Object.class);
        verify(delegate).put(key.capture(), value.capture(), anyLong());
        assertLong(key.getValue(), 1);
        assertLong(value.getValue(), 5);

        assertThat(run(proxy, "store.get(7)")).isEqualTo("ok");
        verify(delegate).get(key.capture());
        assertLong(key.getValue(), 7);

        assertThat(run(proxy, "store.put(1, 1.5, 1000)")).isEqualTo("TypeError: Store counts: value must be long, got float");
    }

    @Test
    @DisplayName("Window store: put and fetch convert Python ints to the declared long type")
    void windowStoreConvertsInts() {
        final WindowStore<Object, Object> delegate = mock(WindowStore.class);
        when(delegate.name()).thenReturn("counts");
        final var proxy = new WindowStoreProxy(delegate, LONG, LONG);

        assertThat(run(proxy, "store.put(1, 5, 0)")).isEqualTo("ok");
        final var key = ArgumentCaptor.forClass(Object.class);
        final var value = ArgumentCaptor.forClass(Object.class);
        verify(delegate).put(key.capture(), value.capture(), anyLong());
        assertLong(key.getValue(), 1);
        assertLong(value.getValue(), 5);

        assertThat(run(proxy, "store.fetch(7, 0)")).isEqualTo("ok");
        verify(delegate).fetch(key.capture(), anyLong());
        assertLong(key.getValue(), 7);

        assertThat(run(proxy, "store.put(1, 1.5, 0)")).isEqualTo("TypeError: Store counts: value must be long, got float");
    }

    @Test
    @DisplayName("Timestamped window store: put and fetch convert Python ints to the declared long type")
    void timestampedWindowStoreConvertsInts() {
        final TimestampedWindowStore<Object, Object> delegate = mock(TimestampedWindowStore.class);
        when(delegate.name()).thenReturn("counts");
        final var proxy = new TimestampedWindowStoreProxy(delegate, LONG, LONG, () -> 1000L);

        assertThat(run(proxy, "store.put(1, 5, 0)")).isEqualTo("ok");
        final var key = ArgumentCaptor.forClass(Object.class);
        final ArgumentCaptor<ValueAndTimestamp<Object>> value = ArgumentCaptor.captor();
        verify(delegate).put(key.capture(), value.capture(), anyLong());
        assertLong(key.getValue(), 1);
        assertLong(value.getValue().value(), 5);

        assertThat(run(proxy, "store.fetch(7, 0)")).isEqualTo("ok");
        verify(delegate).fetch(key.capture(), anyLong());
        assertLong(key.getValue(), 7);

        assertThat(run(proxy, "store.put(1, 1.5, 0)")).isEqualTo("TypeError: Store counts: value must be long, got float");
    }

    @Test
    @DisplayName("Session store: put converts the key inside the session key and the value; fetch converts the key")
    void sessionStoreConvertsInts() {
        final SessionStore<Object, Object> delegate = mock(SessionStore.class);
        when(delegate.name()).thenReturn("counts");
        final var proxy = new SessionStoreProxy(delegate, LONG, LONG);

        assertThat(run(proxy, "store.put({'key': 1, 'start': 3000000000, 'end': 3000000001}, 5)")).isEqualTo("ok");
        final ArgumentCaptor<Windowed<Object>> sessionKey = ArgumentCaptor.captor();
        final var value = ArgumentCaptor.forClass(Object.class);
        verify(delegate).put(sessionKey.capture(), value.capture());
        assertLong(sessionKey.getValue().key(), 1);
        assertLong(value.getValue(), 5);

        assertThat(run(proxy, "store.fetch(7)")).isEqualTo("ok");
        final var key = ArgumentCaptor.forClass(Object.class);
        verify(delegate).fetch(key.capture());
        assertLong(key.getValue(), 7);

        assertThat(run(proxy, "store.put({'key': 1.5, 'start': 3000000000, 'end': 3000000001}, 5)"))
                .isEqualTo("TypeError: Store counts: key must be long, got float");
    }
}
