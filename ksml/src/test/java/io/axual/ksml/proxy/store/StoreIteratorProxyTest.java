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

import io.axual.ksml.python.PythonContext;
import io.axual.ksml.python.PythonContextConfig;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks that the store iterators that {@link ProxyUtil#toPython(Object)} returns work with Python iteration
 * ({@code for}, {@code next()}, {@code list()}, {@code contextlib.closing}) and with the Java style
 * ({@code hasNext()}, {@code next()}, {@code close()}), and that the Kafka iterator is closed (AXPD-11068).
 */
class StoreIteratorProxyTest {

    /** Kafka key-value iterator over fixed entries that records whether it was closed. */
    private static class FakeKeyValueIterator<K> implements KeyValueIterator<K, Object> {
        private final List<KeyValue<K, Object>> entries;
        private int position = 0;
        private boolean closed = false;

        FakeKeyValueIterator(List<KeyValue<K, Object>> entries) {
            this.entries = entries;
        }

        @Override
        public boolean hasNext() {
            return position < entries.size();
        }

        @Override
        public KeyValue<K, Object> next() {
            if (!hasNext()) throw new NoSuchElementException();
            return entries.get(position++);
        }

        @Override
        public K peekNextKey() {
            if (!hasNext()) throw new NoSuchElementException();
            return entries.get(position).key;
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    /** Kafka window store iterator: the key of each entry is the window start time. */
    private static class FakeWindowStoreIterator extends FakeKeyValueIterator<Long> implements WindowStoreIterator<Object> {
        FakeWindowStoreIterator(List<KeyValue<Long, Object>> entries) {
            super(entries);
        }
    }

    private PythonContext pythonContext;
    private FakeKeyValueIterator<?> kafkaIterator;

    @BeforeEach
    void setUp() {
        pythonContext = new PythonContext(PythonContextConfig.builder().build());
        kafkaIterator = new FakeKeyValueIterator<>(List.of(
                KeyValue.pair("a", "1"), KeyValue.pair("b", "2"), KeyValue.pair("c", "3")));
    }

    @AfterEach
    void tearDown() {
        pythonContext.close();
    }

    /** Runs the Python code with {@code it} bound to the proxied Kafka iterator, and returns the last expression. */
    private Value run(String code) {
        final var bindings = pythonContext.context().getBindings("python");
        bindings.putMember("it", ProxyUtil.toPython(kafkaIterator));
        return pythonContext.context().eval("python", code);
    }

    private static List<String> strings(Value list) {
        final var result = new ArrayList<String>();
        for (long i = 0; i < list.getArraySize(); i++) result.add(list.getArrayElement(i).asString());
        return result;
    }

    @Test
    @DisplayName("a for loop returns all entries as {key, value} dicts and closes the Kafka iterator")
    void forLoopReturnsAllEntriesAndCloses() {
        final var result = run("""
                [e["key"] + "=" + e["value"] for e in it]
                """);
        assertThat(strings(result)).containsExactly("a=1", "b=2", "c=3");
        assertThat(kafkaIterator.closed).isTrue();
    }

    @Test
    @DisplayName("each entry of a for loop is a real Python dict")
    void forLoopEntriesAreDicts() {
        final var result = run("""
                all(type(e) is dict for e in it)
                """);
        assertThat(result.asBoolean()).isTrue();
    }

    @Test
    @DisplayName("the builtin next() raises StopIteration after the last entry and closes the Kafka iterator")
    void builtinNextRaisesStopIterationAtEnd() {
        final var result = run("""
                def read():
                    out = [next(it)["key"], next(it)["key"], next(it)["key"]]
                    try:
                        next(it)
                        out.append("no StopIteration")
                    except StopIteration:
                        out.append("StopIteration")
                    return out
                read()
                """);
        assertThat(strings(result)).containsExactly("a", "b", "c", "StopIteration");
        assertThat(kafkaIterator.closed).isTrue();
    }

    @Test
    @DisplayName("list() returns all entries and closes the Kafka iterator")
    void listReturnsAllEntriesAndCloses() {
        final var result = run("""
                [e["key"] for e in list(it)]
                """);
        assertThat(strings(result)).containsExactly("a", "b", "c");
        assertThat(kafkaIterator.closed).isTrue();
    }

    @Test
    @DisplayName("the hasNext()/next()/close() style still works (regression)")
    void javaStyleStillWorks() {
        final var result = run("""
                def read():
                    out = []
                    while it.hasNext():
                        out.append(it.next()["key"])
                    it.close()
                    return out
                read()
                """);
        assertThat(strings(result)).containsExactly("a", "b", "c");
        assertThat(kafkaIterator.closed).isTrue();
    }

    @Test
    @DisplayName("a hasNext() loop that reads all entries closes the Kafka iterator without close()")
    void javaStyleClosesWhenUsedUp() {
        run("""
                while it.hasNext():
                    it.next()
                """);
        assertThat(kafkaIterator.closed).isTrue();
    }

    @Test
    @DisplayName("contextlib.closing closes the Kafka iterator when the loop ends early with break")
    void contextlibClosingClosesOnBreak() {
        final var result = run("""
                import contextlib
                def read():
                    with contextlib.closing(it) as entries:
                        for e in entries:
                            return e["key"]
                read()
                """);
        assertThat(result.asString()).isEqualTo("a");
        assertThat(kafkaIterator.closed).isTrue();
    }

    @Test
    @DisplayName("contextlib.closing closes the Kafka iterator when the loop raises an exception")
    void contextlibClosingClosesOnException() {
        final var result = run("""
                import contextlib
                def read():
                    try:
                        with contextlib.closing(it) as entries:
                            for e in entries:
                                raise ValueError("stop")
                    except ValueError as e:
                        return str(e)
                read()
                """);
        assertThat(result.asString()).isEqualTo("stop");
        assertThat(kafkaIterator.closed).isTrue();
    }

    @Test
    @DisplayName("peekNextKey() returns the next key without moving the iterator forward")
    void peekNextKeyDoesNotAdvance() {
        final var result = run("""
                [it.peekNextKey(), it.peekNextKey(), it.next()["key"], it.peekNextKey()]
                """);
        assertThat(strings(result)).containsExactly("a", "a", "a", "b");
    }

    @Test
    @DisplayName("a window store iterator works in a for loop, with the window start time as key, and is closed")
    void windowStoreIteratorForLoop() {
        kafkaIterator = new FakeWindowStoreIterator(List.of(
                KeyValue.pair(1000L, "x"), KeyValue.pair(2000L, "y")));
        final var result = run("""
                [str(e["key"]) + "=" + e["value"] for e in it]
                """);
        assertThat(strings(result)).containsExactly("1000=x", "2000=y");
        assertThat(kafkaIterator.closed).isTrue();
    }

    @Test
    @DisplayName("an empty iterator gives an empty for loop and is closed")
    void emptyIteratorForLoop() {
        kafkaIterator = new FakeKeyValueIterator<>(List.of());
        final var result = run("""
                [e for e in it]
                """);
        assertThat(result.getArraySize()).isZero();
        assertThat(kafkaIterator.closed).isTrue();
    }
}
