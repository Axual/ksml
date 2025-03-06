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
import io.axual.ksml.data.util.ConvertUtil;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.type.UserType;

// This DataObjectConverter makes expected data types compatible with the actual data that was
// created. It does so by converting numbers to strings, and vice versa. It converts complex
// data objects like Enums, Lists and Structs too, recursively going through sub-elements if
// necessary. When no standard conversion is possible, the mechanism reverts to using the
// source or target notation's converter to respectively export or import the data type to
// the desired type.
public class DataObjectConverter {
    private static final NativeDataObjectMapper OBJECT_MAPPER = new NativeDataObjectMapper();
    private static final DataTypeDataSchemaMapper SCHEMA_MAPPER = new DataTypeFlattener();
    private final ConvertUtil convertUtil;

    public DataObjectConverter() {
        convertUtil = new ConvertUtil(OBJECT_MAPPER, SCHEMA_MAPPER);
    }

    public DataObject convert(String sourceNotation, DataObject value, UserType targetType) {
        return convertUtil.convertDataObject(
                ExecutionContext.INSTANCE.notationLibrary().getIfExists(sourceNotation),
                ExecutionContext.INSTANCE.notationLibrary().getIfExists(targetType.notation()),
                targetType.dataType(),
                value,
                false);
    }
}
