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


import io.axual.ksml.data.validation.ValidationContext;
import io.axual.ksml.data.validation.ValidationResult;
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
    public ValidationResult checkAssignableFrom(final DataType other, final ValidationContext context) {
        if (other instanceof SimpleType simpleType) {
            return checkAssignableFrom(simpleType.containerClass, context);
        } else if (other != null) {
            return context.typeMismatch(this, other);
        } else {
            return context.addError("No type specified, this is a bug in KSML");
        }
    }

    @Override
    public ValidationResult checkAssignableFrom(final Class<?> otherContainerClass, final ValidationContext context) {
        if (!containerClass.isAssignableFrom(otherContainerClass)) {
            return context.typeMismatch(this, otherContainerClass);
        }
        return context;
    }

    public boolean equals(Object obj) {
        if (this == obj) return true;
        return obj != null
                && getClass().equals(obj.getClass())
                && containerClass.equals(((SimpleType) obj).containerClass);
    }

    public int hashCode() {
        return Objects.hash(super.hashCode(), containerClass.hashCode());
    }
}
