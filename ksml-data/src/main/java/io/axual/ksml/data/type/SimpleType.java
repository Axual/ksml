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


import io.axual.ksml.data.compare.Assignable;
import io.axual.ksml.data.compare.Equal;
import io.axual.ksml.data.util.EqualsUtil;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import static io.axual.ksml.data.type.DataTypeFlags.IGNORE_DATA_TYPE_CONTAINER_CLASS;
import static io.axual.ksml.data.util.AssignableUtil.fieldNotAssignable;
import static io.axual.ksml.data.util.AssignableUtil.typeMismatch;
import static io.axual.ksml.data.util.EqualsUtil.otherIsNull;

/**
 * A concrete {@link DataType} backed by a single Java container class.
 * <p>
 * SimpleType represents scalar/primitive-like types where assignability is based on the
 * assignability of the configured {@code containerClass}.
 */
@EqualsAndHashCode(exclude = {"name", "spec"})
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
    public Assignable isAssignableFrom(final DataType type) {
        if (!(type instanceof SimpleType that))
            return typeMismatch(this, type);

        // Check containerClass
        if (!containerClass.isAssignableFrom(that.containerClass))
            return fieldNotAssignable("containerClass", this, containerClass, that, that.containerClass);

        return Assignable.ok();
    }

    /**
     * Checks if this schema type is equal to another schema. Equality checks are parameterized by flags passed in.
     *
     * @param other The other schema to compare.
     * @param flags The flags that indicate what to compare.
     */
    @Override
    public Equal equals(Object other, Flags flags) {
        if (this == other) return Equal.ok();
        if (other == null) return otherIsNull(this);
        if (!getClass().equals(other.getClass()))
            return EqualsUtil.containerClassNotEqual(getClass(), other.getClass());

        final var that = (SimpleType) other;

        // Compare containerClass
        if (!flags.isSet(IGNORE_DATA_TYPE_CONTAINER_CLASS) && !containerClass.equals(that.containerClass))
            return EqualsUtil.containerClassNotEqual(containerClass, that.containerClass);

        return Equal.ok();
    }
}
