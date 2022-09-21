package io.axual.ksml.data.object;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 Axual B.V.
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

import java.util.Objects;

import io.axual.ksml.data.type.DataType;

public class DataPrimitive<T> implements DataObject {
    private final DataType type;
    private final T value;

    public DataPrimitive(DataType type, T value) {
        this.type = type;
        this.value = value;
    }

    @Override
    public DataType type() {
        return type;
    }

    public T value() {
        return value;
    }

    @Override
    public String toString() {
        return value != null ? value.toString() : "null";
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (other == null || other.getClass() != getClass()) return false;
        DataPrimitive<?> o = (DataPrimitive<?>) other;
        if (!type.equals(o.type)) return false;
        if (value == null) return o.value == null;
        return value.equals(o.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, value);
    }
}
