package io.axual.ksml.store;

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

import io.axual.ksml.python.PythonTypeConverter;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.graalvm.polyglot.HostAccess;

/**
 * A proxy wrapper around a Kafka Streams ValueAndTimestamp that exposes its methods
 * to Python code via @HostAccess.Export annotations.
 * <p>
 * Since ValueAndTimestamp is a final class and cannot be extended, this proxy uses
 * composition to delegate all calls to the wrapped instance.
 *
 * @param <V> the type of the value
 */
public class ValueAndTimestampProxy<V> {
    
    private final ValueAndTimestamp<V> delegate;

    public ValueAndTimestampProxy(ValueAndTimestamp<V> delegate) {
        this.delegate = delegate;
    }

    /**
     * Returns the value of this record, converted to a Python-compatible type.
     *
     * @return the value
     */
    @HostAccess.Export
    public Object value() {
        return PythonTypeConverter.toPython(delegate.value());
    }

    /**
     * Returns the timestamp of this record.
     *
     * @return the timestamp
     */
    @HostAccess.Export
    public long timestamp() {
        return delegate.timestamp();
    }

    /**
     * Return the underlying ValueAndTimestamp instance.
     * This method is not exposed to Python code.
     * @return the delegate.
     */
    public ValueAndTimestamp<V> delegate() {
        return delegate;
    }

    @Override
    @HostAccess.Export
    public String toString() {
        return delegate.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof ValueAndTimestampProxy<?> other) {
            return delegate.equals(other.delegate);
        }
        if (o instanceof ValueAndTimestamp<?>) {
            return delegate.equals(o);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }
}
