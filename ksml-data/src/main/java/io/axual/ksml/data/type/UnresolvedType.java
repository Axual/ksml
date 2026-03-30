package io.axual.ksml.data.type;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
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

import io.axual.ksml.data.compare.Assignable;
import io.axual.ksml.data.compare.Equality;
import io.axual.ksml.data.compare.EqualityFlags;

/**
 * Marker type that signals a deferred schema resolution.
 * <p>
 * When a stream definition uses a notation without an explicit schema name (e.g., {@code confluent_avro}
 * instead of {@code avro:SensorData}), an UnresolvedType is used as a placeholder. It must be resolved
 * before topology construction by fetching the schema from a schema registry using the topic name.
 * <p>
 * This type is not assignable from or to any other type. If it leaks past the resolution phase,
 * any assignment check will fail with a clear error message.
 */
public class UnresolvedType implements DataType {
    public static final UnresolvedType INSTANCE = new UnresolvedType();

    private UnresolvedType() {
    }

    @Override
    public Class<?> containerClass() {
        return Object.class;
    }

    @Override
    public String name() {
        return "Unresolved";
    }

    @Override
    public String spec() {
        return "unresolved";
    }

    @Override
    public Assignable isAssignableFrom(DataType type) {
        return Assignable.notAssignable("Unresolved type has not been resolved from schema registry");
    }

    @Override
    public Equality equals(Object obj, EqualityFlags flags) {
        if (this == obj) return Equality.equal();
        return Equality.notEqual("UnresolvedType is not equal to " + (obj != null ? obj : "null"));
    }

    @Override
    public String toString() {
        return spec();
    }
}
