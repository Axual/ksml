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

import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.type.DataType;

/**
 * Maps between native Java values of type T and the KSML DataObject model.
 * Implementations convert to and from the neutral DataObject representation,
 * optionally guided by an expected DataType.
 *
 * @param <T> The native value type handled by this mapper
 */
public interface DataObjectMapper<T> {
    /**
     * Converts a native value to a DataObject without an expected target type.
     *
     * @param value the native value to convert
     * @return the DataObject representation of the value
     */
    default DataObject toDataObject(T value) {
        return toDataObject(null, value);
    }

    /**
     * Converts a native value to a DataObject, using an expected DataType to guide the conversion.
     * Implementations may coerce or adapt the value to match the expected type.
     *
     * @param expected the expected DataType of the result, or null if unknown
     * @param value    the native value to convert
     * @return the DataObject representation of the value (never null, unless the implementation permits)
     */
    DataObject toDataObject(DataType expected, T value);

    /**
     * Converts a DataObject back to its native representation.
     *
     * @param value the DataObject to convert
     * @return the native value of type T
     */
    T fromDataObject(DataObject value);
}
