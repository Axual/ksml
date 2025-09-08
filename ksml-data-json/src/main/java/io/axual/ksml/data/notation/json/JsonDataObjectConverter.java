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
import io.axual.ksml.data.object.*;
import io.axual.ksml.data.type.*;

/**
 * Converter between KSML DataObjects and JSON text using the {@link JsonDataObjectMapper}.
 *
 * <p>This converter supports two directions:</p>
 * <ul>
 *   <li>Structured JSON DataObjects (DataList, DataMap, DataStruct) -> DataString containing a JSON document</li>
 *   <li>DataString containing JSON -> Structured DataObject of a requested {@link DataType}
 *       ({@link ListType}, {@link MapType}, {@link StructType}, {@link UnionType})</li>
 * </ul>
 *
 * <p>If a conversion is not applicable (eg. trying to convert a non-JSON structure to a list, or
 * an incompatible target type), this converter returns {@code null}.</p>
 */
public class JsonDataObjectConverter implements Notation.Converter {
    /** Mapper that performs the heavy lifting for JSON <-> DataObject conversion. */
    private static final JsonDataObjectMapper DATA_OBJECT_MAPPER = new JsonDataObjectMapper(false);

    /**
     * Convert the given value into the specified target type using JSON as an intermediary when needed.
     *
     * @param value      the input {@link DataObject}, either structured JSON ({@link DataList}, {@link DataMap},
     *                   or {@link DataStruct}) or a {@link DataString} containing a JSON document
     * @param targetType the desired {@link DataType} to convert to
     * @return the converted {@link DataObject}, or {@code null} if no conversion is possible
     */
    @Override
    public DataObject convert(DataObject value, DataType targetType) {
        // Case 1: Convert from structured JSON objects (list/map/struct) to a JSON text (DataString)
        if (value instanceof DataList || value instanceof DataMap || value instanceof DataStruct) {
            // Only allowed target for this direction is a DataString
            if (targetType == DataString.DATATYPE) {
                // Serialize the structured value to a JSON string
                return new DataString(DATA_OBJECT_MAPPER.fromDataObject(value));
            }
        }

        // Case 2: Convert from a JSON text (DataString) to a structured JSON object
        if (value instanceof DataString jsonString) {
            // Only perform conversion when the targetType is a structured JSON type
            if (targetType instanceof ListType || targetType instanceof MapType || targetType instanceof StructType || targetType instanceof UnionType) {
                // Parse the JSON text into the requested DataObject structure
                return DATA_OBJECT_MAPPER.toDataObject(targetType, jsonString.value());
            }
        }

        // If we get here, no conversion was applicable
        return null;
    }
}
