package io.axual.ksml.data.mapper;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
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
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.type.DataType;

/**
 * DataObjectMapper implementation for String values.
 */
public class StringDataObjectMapper implements DataObjectMapper<String> {
    /**
     * Converts a String to a DataString if the expected type is DataString.
     *
     * @param expected the expected DataType
     * @param value    the String value to convert
     * @return a DataString when expected is DataString, otherwise null
     */
    @Override
    public DataObject toDataObject(DataType expected, String value) {
        if (expected != DataString.DATATYPE) return null;
        return new DataString(value);
    }

    /**
     * Converts a DataObject to a String when the value is a DataString.
     *
     * @param value the DataObject to convert
     * @return the String value, or null if not a DataString
     */
    @Override
    public String fromDataObject(DataObject value) {
        if (value instanceof DataString val) return val.value();
        return null;
    }
}
