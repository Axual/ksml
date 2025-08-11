package io.axual.ksml.data.type;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2023 Axual B.V.
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


import lombok.Getter;

import java.util.Objects;

/**
 * A concrete {@link DataType} backed by a single Java container class.
 * <p>
 * SimpleType represents scalar/primitive-like types where assignability is based on the
 * assignability of the configured {@code containerClass}.
 */
@Getter
public class SimpleType implements DataType {
    private final Class<?> containerClass;
    private final String name;
    private final String spec;

    public SimpleType(Class<?> containerClass, String name) {
        this(containerClass, name, name);
    }

    public SimpleType(Class<?> containerClass, String name, String spec) {
        this.containerClass = containerClass;
        this.name = name;
        this.spec = spec;
    }

    @Override
    public String toString() {
        return spec;
    }

    @Override
    public boolean isAssignableFrom(DataType other) {
        if (other instanceof SimpleType simpleType) {
            return isAssignableFrom(simpleType.containerClass);
        }
        return false;
    }

    @Override
    public boolean isAssignableFrom(Class<?> type) {
        return containerClass.isAssignableFrom(type);
    }

    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        SimpleType other = (SimpleType) obj;
        return isAssignableFrom(other) && other.isAssignableFrom(this);
    }

    public int hashCode() {
        return Objects.hash(super.hashCode(), containerClass.hashCode());
    }
}
