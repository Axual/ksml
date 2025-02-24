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

import io.axual.ksml.data.type.UnionType;

/**
 * Represents a wrapper for a boolean value as part of the {@link DataObject} framework.
 *
 * <p>The {@code DataBoolean} class encapsulates a boolean value to integrate seamlessly
 * into the structured data model used in schema-compliant or stream-processed data.
 * It enables boolean values to be used as {@link DataObject} types, making them compatible
 * with the framework and allowing for standardized processing.</p>
 *
 * @see DataObject
 */
public class DataUnion extends DataPrimitive<DataObject> {
    public DataUnion(UnionType type, DataObject value) {
        super(type, value);
    }

    @Override
    public String toString(Printer printer) {
        return printer.schemaString(this) + (value() != null ? value().toString(printer.childObjectPrinter()) : "null");
    }
}
