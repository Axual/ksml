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
import io.axual.ksml.data.compare.FilteredEquals;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;

import static io.axual.ksml.data.util.AssignableUtil.fieldNotAssignable;
import static io.axual.ksml.data.util.AssignableUtil.typeMismatch;

/**
 * Describes a KSML logical data type.
 * <p>
 * Implementations provide metadata such as a human-readable name and "spec" representation,
 * the underlying Java container class, and rules to determine assignability from other
 * {@code DataType} instances, Java {@code Class} objects, or runtime values.
 * <p>
 * The {@link #UNKNOWN} constant acts as a wildcard type that is assignable from any other type
 * or value.
 */
public interface DataType extends FilteredEquals {
    Class<?> containerClass();

    String name();

    String spec();

    Assignable isAssignableFrom(final DataType type);

    default Assignable isAssignableFrom(DataObject value) {
        // Always allow a null value to be assigned
        if (value == DataNull.INSTANCE) return Assignable.ok();
        // If not NULL, check the value type for assignability
        return isAssignableFrom(value.type());
    }

    default Assignable isAssignableFrom(Class<?> otherClass) {
        if (!containerClass().isAssignableFrom(otherClass))
            return typeMismatch(this, otherClass);
        return Assignable.ok();
    }

    default Assignable isAssignableFrom(Object value) {
        // Always allow a null value to be assigned
        if (value == null) return Assignable.ok();

        // Check containerClass
        final var containerClassAssignable = isAssignableFrom(value.getClass());
        if (containerClassAssignable.isError())
            return fieldNotAssignable("containerClass", this, containerClass(), value, value.getClass(), containerClassAssignable);

        return Assignable.ok();
    }

    DataType UNKNOWN = new DataType() {
        @Override
        public Class<?> containerClass() {
            return Object.class;
        }

        public String name() {
            return "Unknown";
        }

        public String spec() {
            return "?";
        }

        @Override
        public Assignable isAssignableFrom(DataType type) {
            // Do nothing to indicate any data type is assignable
            return Assignable.ok();
        }

        @Override
        public Assignable isAssignableFrom(Object value) {
            // Do nothing to indicate any value class is assignable
            return Assignable.ok();
        }

        @Override
        public Equal equals(Object obj, Flags flags) {
            if (this == obj) return Equal.ok();
            return Equal.error("Type \"" + this + "\" is not type \"" + (obj != null ? obj : "null") + "\"");
        }

        @Override
        public String toString() {
            return spec();
        }
    };
}
