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

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.mapper.DataSchemaMapper;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.schema.NativeDataSchemaMapper;

import javax.annotation.Nullable;
import java.util.Map;

public class NativeDataObjectMapperWithSchema extends NativeDataObjectMapper {
    public static final String STRUCT_SCHEMA_FIELD = DataStruct.META_ATTRIBUTE_CHAR + "schema";
    public static final String STRUCT_TYPE_FIELD = DataStruct.META_ATTRIBUTE_CHAR + "type";

    private static final DataSchemaMapper<Object> NATIVE_DATA_SCHEMA_MAPPER = new NativeDataSchemaMapper();
    private final boolean includeSchemaInfo;

    public NativeDataObjectMapperWithSchema(boolean includeSchemaInfo) {
        this.includeSchemaInfo = includeSchemaInfo;
    }

    @Override
    protected StructType inferStructTypeFromNative(Map<?, ?> map, DataSchema expected) {
        final var schema = inferStructSchemaFromNative(map);
        if (schema instanceof StructSchema structSchema) return new StructType(structSchema);
        if (schema != null) throw new DataException("Map can not be converted to " + schema);
        return super.inferStructTypeFromNative(map, expected);
    }

    private DataSchema inferStructSchemaFromNative(Map<?, ?> map) {
        // Find out the schema of the struct by looking at the fields. If there are no meta fields
        // to override the expected schema, then return the expected schema.

        // The "@schema" field overrides the entire schema library. If this field is filled, then
        // we assume the entire schema is contained within the map itself. Therefore, we do not
        // consult the schema library, but instead directly decode the schema from the field.
        if (map.containsKey(STRUCT_SCHEMA_FIELD)) {
            final var nativeSchema = map.get(STRUCT_SCHEMA_FIELD);
            return NATIVE_DATA_SCHEMA_MAPPER.toDataSchema("dummy", nativeSchema);
        }

        // The "@type" field indicates a type that is assumed to be contained in the schema
        // library. If this field is set, then look up the schema by name in the library.
        if (map.containsKey(STRUCT_TYPE_FIELD)) {
            final var schemaName = map.get(STRUCT_TYPE_FIELD).toString();
            final var schema = loadSchemaByName(schemaName);
            if (schema != null) return schema;
        }

        // No fields found to infer the schema
        return null;
    }

    @Override
    protected DataObject convertNativeToDataStruct(Map<String, Object> map, StructType type) {
        map.remove(STRUCT_SCHEMA_FIELD);
        map.remove(STRUCT_TYPE_FIELD);
        return super.convertNativeToDataStruct(map, type);
    }

    @Nullable
    @Override
    public Map<String, Object> convertDataStructToNative(DataStruct struct) {
        final var result = super.convertDataStructToNative(struct);
        if (result == null) return null;

        // Convert schema to native format by encoding it in meta fields
        final var schema = struct.type().schema();
        if (schema != null && includeSchemaInfo) {
            result.put(STRUCT_TYPE_FIELD, schema.name());
            result.put(STRUCT_SCHEMA_FIELD, NATIVE_DATA_SCHEMA_MAPPER.fromDataSchema(schema));
        }

        // Return the native representation as Map
        return result;
    }
}
