package io.axual.ksml.python;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
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

import io.axual.ksml.data.mapper.DataSchemaMapper;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.data.value.Struct;
import io.axual.ksml.schema.NativeDataSchemaMapper;

import javax.annotation.Nullable;
import java.util.Map;

public class NativeDataObjectMapperWithSchema extends NativeDataObjectMapper {
    public static final String STRUCT_SCHEMA_FIELD = Struct.META_ATTRIBUTE_CHAR + "schema";
    public static final String STRUCT_TYPE_FIELD = Struct.META_ATTRIBUTE_CHAR + "type";

    private static final DataSchemaMapper<Object> NATIVE_DATA_SCHEMA_MAPPER = new NativeDataSchemaMapper();
    private final NativeDataObjectMapper recursiveDataObjectMapper;
    private final boolean includeSchemaInfo;

    public NativeDataObjectMapperWithSchema(boolean includeSchemaInfo, NativeDataObjectMapper recursiveDataObjectMapper) {
        this.includeSchemaInfo = includeSchemaInfo;
        this.recursiveDataObjectMapper = recursiveDataObjectMapper;
    }

    @Override
    protected DataObject convertMapToDataStruct(Map<String, Object> map, StructType type) {
        map.remove(STRUCT_SCHEMA_FIELD);
        map.remove(STRUCT_TYPE_FIELD);
        return super.convertMapToDataStruct(map, type);
    }

    @Nullable
    @Override
    public Map<String, Object> convertDataStructToMap(DataStruct struct) {
        // Don't call the superclass, but divert all recursions into nested structures to a separate mapper to prevent
        // recursive inclusion of schema info
        final var result = recursiveDataObjectMapper != null ? recursiveDataObjectMapper.convertDataStructToMap(struct) : super.convertDataStructToMap(struct);
        if (result == null) return null;

        if (includeSchemaInfo) {
            // Convert schema to native format by encoding it in metadata fields
            final var schema = struct.type().schema();
            if (schema != null) {
                result.put(STRUCT_TYPE_FIELD, schema.name());
                result.put(STRUCT_SCHEMA_FIELD, NATIVE_DATA_SCHEMA_MAPPER.fromDataSchema(schema));
            }
        }

        // Return the native representation as Map
        return result;
    }
}
