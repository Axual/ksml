package io.axual.ksml.notation.json;

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

import io.axual.ksml.data.object.*;
import io.axual.ksml.data.parser.NotationConverter;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.data.type.UnionType;
import io.axual.ksml.data.type.UserType;

public class JsonDataObjectConverter implements NotationConverter {
    private static final JsonDataObjectMapper DATA_OBJECT_MAPPER = new JsonDataObjectMapper();

    @Override
    public DataObject convert(DataObject value, UserType targetType) {
        // Convert from structured JSON
        if (value instanceof DataList || value instanceof DataStruct || value instanceof DataUnion) {
            // Convert to String
            if (targetType.dataType() == DataString.DATATYPE) {
                return new DataString(DATA_OBJECT_MAPPER.fromDataObject(value));
            }
        }

        // Convert from String
        if (value instanceof DataString jsonString) {
            // Convert to structured JSON
            if (targetType.dataType() instanceof ListType || targetType.dataType() instanceof StructType || targetType.dataType() instanceof UnionType) {
                return DATA_OBJECT_MAPPER.toDataObject(jsonString.value());
            }
        }

        // Return null if there is no conversion possible
        return null;
    }
}
