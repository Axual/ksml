package io.axual.ksml.data.schema.logical;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2026 Axual B.V.
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

import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.type.DataType;

/** A semantic tag on a base primitive schema (Avro-style), for example decimal on bytes or uuid on string. */
public interface LogicalType {
    String name();

    /** The primitive that carries this logical type on the wire. */
    DataSchema baseSchema();

    /** The runtime type the value takes inside KSML; usually the base type, but string for decimal. */
    DataType representationType();

    /** The primitive schema matching {@link #representationType()}; usually the base, but string for decimal. */
    DataSchema representationSchema();

    /** Validates a value against the semantic. A null value is always allowed; throws DataException otherwise. */
    void validate(DataObject value);
}
