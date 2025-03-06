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

import io.axual.ksml.data.type.DataType;

public interface DataObject {
    DataType type();

    enum Printer {
        INTERNAL,
        EXTERNAL_NO_SCHEMA,
        EXTERNAL_TOP_SCHEMA,
        EXTERNAL_ALL_SCHEMA;

        public Printer childObjectPrinter() {
            if (this == INTERNAL) return EXTERNAL_NO_SCHEMA;
            return this == EXTERNAL_ALL_SCHEMA ? EXTERNAL_ALL_SCHEMA : EXTERNAL_NO_SCHEMA;
        }

        public String forceSchemaString(DataObject value) {
            return value.type().name() + ": ";
        }

        public String schemaString(DataObject value) {
            if (this == INTERNAL || this == EXTERNAL_NO_SCHEMA) return "";
            return forceSchemaString(value);
        }
    }

    String toString(Printer printer);
}
