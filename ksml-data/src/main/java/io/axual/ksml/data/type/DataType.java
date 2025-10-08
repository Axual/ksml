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
import io.axual.ksml.data.compare.Equals;
import io.axual.ksml.data.compare.IsAssignableFrom;

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
public interface DataType extends Equals, IsAssignableFrom {
    Class<?> containerClass();

    String name();

    String spec();

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
        public Compared checkAssignableFrom(DataType type) {
            // Do nothing to indicate any data type is assignable
            return Compared.ok();
        }

        @Override
        public Compared checkAssignableFrom(Class<?> type) {
            // Do nothing to indicate any value class is assignable
            return Compared.ok();
        }

        @Override
        public Compared equals(Object obj, Flags flags) {
            if (this == obj) return Compared.ok();
            return Compared.error("Type \"" + this + "\" is not type \"" + (obj != null ? obj : "null") + "\"");
        }

        @Override
        public String toString() {
            return spec();
        }
    };
}
