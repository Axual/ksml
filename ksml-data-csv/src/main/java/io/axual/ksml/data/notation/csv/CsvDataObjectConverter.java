package io.axual.ksml.data.notation.csv;

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

import io.axual.ksml.data.notation.NotationConverter;
import io.axual.ksml.data.notation.UserType;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.data.type.UnionType;

import static io.axual.ksml.data.notation.csv.CsvNotation.DEFAULT_TYPE;

public class CsvDataObjectConverter implements NotationConverter {
    private static final CsvDataObjectMapper MAPPER = new CsvDataObjectMapper();

    @Override
    public DataObject convert(DataObject value, UserType targetType) {
        // Convert from structured CSV
        if (value instanceof DataList valueList && DEFAULT_TYPE.equals(valueList.type())) {
            // Convert to String
            if (targetType.dataType() == DataString.DATATYPE) {
                return new DataString(MAPPER.fromDataObject(value));
            }
        }

        // Convert from String
        if (value instanceof DataString csvString) {
            // Convert to structured CSV
            if (targetType.dataType() instanceof ListType
                    || targetType.dataType() instanceof StructType
                    || targetType.dataType() instanceof UnionType) {
                return MAPPER.toDataObject(targetType.dataType(), csvString.value());
            }
        }

        // Return null if there is no conversion possible
        return null;
    }
}
