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
import io.axual.ksml.data.util.JsonNodeUtil;

public class JsonSchemaDataObjectMapper implements DataObjectMapper<Object> {
    private final NativeDataObjectMapper nativeMapper;

    public JsonSchemaDataObjectMapper(NativeDataObjectMapper nativeMapper) {
        this.nativeMapper = nativeMapper;
    }

    @Override
    public DataObject toDataObject(DataType expected, Object value) {
        if (value instanceof JsonNode jsonNode) {
            return nativeMapper.toDataObject(expected, JsonNodeUtil.convertJsonNodeToNative(jsonNode));
        }
        throw new DataException("Cannot convert value to DataObject: " + value);
    }

    @Override
    public Object fromDataObject(DataObject value) {
        return JsonNodeUtil.convertNativeToJsonNode(nativeMapper.fromDataObject(value));
    }
}
