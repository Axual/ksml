package io.axual.ksml.data.notation.jsonschema;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - JSON Schema
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
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

import com.fasterxml.jackson.databind.JsonNode;
import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.mapper.DataObjectMapper;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.util.ConvertUtil;
import io.axual.ksml.data.util.JsonNodeUtil;

/**
 * Maps between Jackson {@link JsonNode} trees and KSML {@link DataObject}s for JSON Schema usage.
 *
 * <p>This mapper serves vendor-backed JSON Schema serdes that choose to expose
 * parsed JSON values as Jackson JsonNode at their boundary. It converts:</n>
 * <ul>
 *   <li>JsonNode -> native Map/List/primitives -> DataObject using {@link NativeDataObjectMapper}</li>
 *   <li>DataObject -> native Map/List/primitives -> JsonNode</li>
 * </ul>
 *
 * <p>Behavioral notes:</p>
 * <ul>
 *   <li>Null input maps to a {@code DataNull} according to the expected type (via {@link ConvertUtil}).</li>
 *   <li>Only JsonNode inputs are supported for {@link #toDataObject(DataType, Object)}; other types cause a {@link DataException}.</li>
 * </ul>
 */
public class JsonSchemaDataObjectMapper implements DataObjectMapper<Object> {
    private final NativeDataObjectMapper nativeMapper;

    /** Creates a mapper using the provided {@link NativeDataObjectMapper} for native conversions. */
    public JsonSchemaDataObjectMapper(NativeDataObjectMapper nativeMapper) {
        this.nativeMapper = nativeMapper;
    }

    /**
     * Converts an input value to a {@link DataObject}. Only {@link JsonNode} or {@code null} are accepted.
     */
    @Override
    public DataObject toDataObject(DataType expected, Object value) {
        if (value == null) {
            // Allow nulls (eg. Kafka tombstones), honoring the expected type.
            return ConvertUtil.convertNullToDataObject(expected);
        }
        if (value instanceof JsonNode jsonNode) {
            // First convert JsonNode to native Java (Map/List/primitives), then to DataObject.
            return nativeMapper.toDataObject(expected, JsonNodeUtil.convertJsonNodeToNative(jsonNode));
        }
        // Keep error concise; include the value to aid debugging in tests.
        throw new DataException("Cannot convert value to DataObject: " + value);
    }

    /**
     * Converts a {@link DataObject} to a Jackson {@link JsonNode} tree by first mapping to native Java.
     */
    @Override
    public Object fromDataObject(DataObject value) {
        // Map DataObject -> native -> JsonNode
        return JsonNodeUtil.convertNativeToJsonNode(nativeMapper.fromDataObject(value));
    }
}
