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


import io.axual.ksml.data.compare.Compared;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import static io.axual.ksml.data.type.EqualityFlags.IGNORE_DATA_TYPE_CONTAINER_CLASS;

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
    public Compared checkAssignableFrom(final DataType other) {
        if (other instanceof SimpleType simpleType) return checkAssignableFrom(simpleType.containerClass);
        return Compared.typeMismatch(this, other);
    }

    @Override
    public Compared checkAssignableFrom(final Class<?> otherContainerClass) {
        if (containerClass.isAssignableFrom(otherContainerClass)) return Compared.ok();
        return Compared.typeMismatch(this, otherContainerClass);
    }

    /**
     * Checks if this schema type is equal to another schema. Equality checks are parameterized by flags passed in.
     *
     * @param other The other schema to compare.
     * @param flags The flags that indicate what to compare.
     */
    @Override
    public Compared equals(Object other, Flags flags) {
        if (this == other) return Compared.ok();
        if (other == null) return Compared.otherIsNull(this);
        if (!getClass().equals(other.getClass())) return Compared.notEqual(getClass(), other.getClass());

        final var that = (SimpleType) other;

        // Compare containerClass
        if (!flags.isSet(IGNORE_DATA_TYPE_CONTAINER_CLASS) && !containerClass.equals(that.containerClass))
            return Compared.notEqual(containerClass, that.containerClass);

        return Compared.ok();
    }
}
