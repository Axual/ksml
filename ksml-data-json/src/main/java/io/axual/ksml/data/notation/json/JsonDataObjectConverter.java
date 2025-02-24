package io.axual.ksml.data.notation.json;

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

import io.axual.ksml.data.notation.Notation;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.data.type.UnionType;

public class JsonDataObjectConverter implements Notation.Converter {
    private static final JsonDataObjectMapper DATA_OBJECT_MAPPER = new JsonDataObjectMapper(false);

    @Override
    public DataObject convert(DataObject value, DataType targetType) {
        // Convert from structured JSON
        if (value instanceof DataList || value instanceof DataStruct) {
            // Convert to String
            if (targetType == DataString.DATATYPE) {
                return new DataString(DATA_OBJECT_MAPPER.fromDataObject(value));
            }
        }

        // Convert from String
        if (value instanceof DataString jsonString) {
            // Convert to structured JSON
            if (targetType instanceof ListType || targetType instanceof StructType || targetType instanceof UnionType) {
                return DATA_OBJECT_MAPPER.toDataObject(jsonString.value());
            }
        }

        // Return null if there is no conversion possible
        return null;
    }
}
