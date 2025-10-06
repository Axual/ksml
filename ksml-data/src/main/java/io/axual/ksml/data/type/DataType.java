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


import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.validation.ValidationContext;
import io.axual.ksml.data.validation.ValidationResult;

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
public interface DataType {
    String name();

    String spec();

    default ValidationResult checkAssignableFrom(DataType type) {
        return checkAssignableFrom(type, new ValidationContext());
    }

    ValidationResult checkAssignableFrom(final DataType type, final ValidationContext context);

    default ValidationResult checkAssignableFrom(DataObject value) {
        return checkAssignableFrom(value, new ValidationContext());
    }

    default ValidationResult checkAssignableFrom(DataObject value, ValidationContext context) {
        // Always allow a null value to be assigned
        if (value == DataNull.INSTANCE) return context;
        // If not NULL, check the value type for assignability
        return checkAssignableFrom(value.type(), context);
    }

    default ValidationResult checkAssignableFrom(Class<?> type) {
        return checkAssignableFrom(type, new ValidationContext());
    }

    ValidationResult checkAssignableFrom(Class<?> type, ValidationContext context);

    default ValidationResult checkAssignableFrom(Object value) {
        return checkAssignableFrom(value, new ValidationContext());
    }

    default ValidationResult checkAssignableFrom(Object value, ValidationContext context) {
        // Always allow a null value to be assigned
        if (value == null) return context;
        // If not NULL, check the value class for assignability
        return checkAssignableFrom(value.getClass(), context);
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
        public ValidationResult checkAssignableFrom(DataType type, ValidationContext context) {
            // Do nothing to indicate any data type is assignable
            return context;
        }

        @Override
        public ValidationResult checkAssignableFrom(Class<?> type, ValidationContext context) {
            // Do nothing to indicate any value class is assignable
            return context;
        }

        @Override
        public String toString() {
            return spec();
        }
    };
}
