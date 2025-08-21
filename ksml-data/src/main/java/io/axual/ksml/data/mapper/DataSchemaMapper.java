package io.axual.ksml.data.mapper;

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

import io.axual.ksml.data.schema.DataSchema;

/**
 * Maps between native Java values of type T and the KSML DataSchema model.
 * Implementations convert to and from the neutral DataSchema representation,
 * optionally using a namespace and name for schema identification.
 *
 * @param <T> The native value type handled by this schema mapper
 */
public interface DataSchemaMapper<T> {
    /**
     * Converts a native value to a DataSchema, with optional namespace and name.
     *
     * @param namespace optional schema namespace, may be null
     * @param name      optional schema name, may be null
     * @param value     the native value whose schema should be derived
     * @return the corresponding DataSchema
     */
    DataSchema toDataSchema(String namespace, String name, T value);

    /**
     * Converts a native value to a DataSchema using only a name.
     *
     * @param name  the schema name, may be null
     * @param value the native value whose schema should be derived
     * @return the corresponding DataSchema
     */
    default DataSchema toDataSchema(String name, T value) {
        return toDataSchema(null, name, value);
    }

    /**
     * Converts a native value to a DataSchema without a namespace or name.
     *
     * @param value the native value whose schema should be derived
     * @return the corresponding DataSchema
     */
    default DataSchema toDataSchema(T value) {
        return toDataSchema(null, null, value);
    }

    /**
     * Converts a DataSchema back to its native representation.
     *
     * @param schema the DataSchema to convert
     * @return the native value of type T that corresponds to the schema
     */
    T fromDataSchema(DataSchema schema);
}
