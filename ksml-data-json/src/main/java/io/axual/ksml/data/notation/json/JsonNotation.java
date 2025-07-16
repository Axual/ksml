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

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.notation.BaseNotation;
import io.axual.ksml.data.type.*;
import org.apache.kafka.common.serialization.Serde;

public class JsonNotation extends BaseNotation {
    public static final DataType DEFAULT_TYPE = new UnionType(
            new UnionType.MemberType(new StructType()),
            new UnionType.MemberType(new ListType()));
    private final NativeDataObjectMapper nativeMapper;

    public JsonNotation(String name, NativeDataObjectMapper nativeMapper) {
        super(name, ".json", DEFAULT_TYPE, new JsonDataObjectConverter(), new JsonSchemaLoader());
        this.nativeMapper = nativeMapper;
    }

    @Override
    public DataType defaultType() {
        return DEFAULT_TYPE;
    }

    @Override
    public Serde<Object> serde(DataType type, boolean isKey) {
        // JSON types can either be Map (or Struct), or List, or the union type of both Struct and List
        if (type instanceof MapType || type instanceof ListType || JsonNotation.DEFAULT_TYPE.isAssignableFrom(type))
            return new JsonSerde(nativeMapper, type);
        // Other types cannot be serialized as JSON
        throw new DataException("JSON serde not found for data type: " + type);
    }
}
