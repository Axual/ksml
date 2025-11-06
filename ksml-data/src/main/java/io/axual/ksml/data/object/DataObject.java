package io.axual.ksml.data.object;

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

import io.axual.ksml.data.compare.DataEquals;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.compare.EqualityFlags;
import io.axual.ksml.data.compare.Equality;

/**
 * Defines the common abstraction for all values that participate in the KSML data model.
 *
 * <p>Implementations wrap concrete values (primitives and structured types) and carry
 * their {@link DataType} metadata so values can be validated, printed and processed in a
 * schema-aware way across the framework.</p>
 */
public interface DataObject extends DataEquals {
    /**
     * Returns the {@link DataType} that describes this value.
     */
    DataType type();

    /**
     * Checks if this schema type is equal to another schema. Equality checks are parameterized by flags passed in.
     */
    Equality equals(Object obj, EqualityFlags flags);

    /**
     * Printer options controlling how values are rendered to strings.
     */
    enum Printer {
        /**
         * Internal formatting for logs and debug output.
         */
        INTERNAL,
        /**
         * External formatting without schema names.
         */
        EXTERNAL_NO_SCHEMA,
        /**
         * External formatting with schema on the top-level value only.
         */
        EXTERNAL_TOP_SCHEMA,
        /**
         * External formatting with schema for all nested values.
         */
        EXTERNAL_ALL_SCHEMA;

        /**
         * Derives the printer to use for nested child objects relative to this printer.
         */
        public Printer childObjectPrinter() {
            if (this == INTERNAL) return EXTERNAL_NO_SCHEMA;
            return this == EXTERNAL_ALL_SCHEMA ? EXTERNAL_ALL_SCHEMA : EXTERNAL_NO_SCHEMA;
        }

        /**
         * Returns the schema prefix that must always be shown for the given value.
         */
        public String forceSchemaPrefix(DataObject value) {
            return value.type().name() + ": ";
        }

        /**
         * Returns the schema prefix according to the current printer mode.
         */
        public String schemaPrefix(DataObject value) {
            if (this == INTERNAL || this == EXTERNAL_NO_SCHEMA) return "";
            return forceSchemaPrefix(value);
        }
    }

    /**
     * Returns a string representation according to the provided {@link Printer} mode.
     */
    String toString(Printer printer);
}
