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

import org.apache.kafka.streams.state.VersionedRecord;
import org.graalvm.polyglot.HostAccess;

/**
 * A proxy wrapper around a Kafka Streams VersionedRecord that exposes its methods
 * to Python code via @HostAccess.Export annotations.
 * <p>
 * Since VersionedRecord is a final class and cannot be extended, this proxy uses
 * composition and is cast to VersionedRecord for interface compatibility. This works
 * for Python interop where the Java type system is not enforced.
 *
 * @param <V> the type of the value
 */
public class VersionedRecordProxy<V> {
    private final VersionedRecord<V> delegate;

    public VersionedRecordProxy(VersionedRecord<V> delegate) {
        this.delegate = delegate;
    }

    /**
     * Returns the value of this record.
     *
     * @return the value
     */
    @HostAccess.Export
    public V value() {
        return delegate.value();
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

    @Override
    @HostAccess.Export
    public String toString() {
        return delegate.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof VersionedRecordProxy<?> other) {
            return delegate.equals(other.delegate);
        }
        if (o instanceof VersionedRecord<?>) {
            return delegate.equals(o);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }
}
