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

import io.axual.ksml.data.exception.ExecutionException;
import io.axual.ksml.data.loader.SchemaLoader;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.notation.Notation;
import io.axual.ksml.data.notation.NotationConverter;
import io.axual.ksml.data.serde.JsonSerde;
import io.axual.ksml.data.type.*;
import lombok.Getter;
import org.apache.kafka.common.serialization.Serde;

public class JsonNotation implements Notation {
    public static final DataType DEFAULT_TYPE = new UnionType(new StructType(), new ListType());
    private final NativeDataObjectMapper nativeMapper;
    @Getter
    private final NotationConverter converter = new JsonDataObjectConverter();
    @Getter
    private final SchemaLoader loader;

    public JsonNotation(NativeDataObjectMapper nativeMapper, SchemaLoader loader) {
        this.nativeMapper = nativeMapper;
        this.loader = loader;
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
        // Other types can not be serialized as JSON
        throw new ExecutionException("JSON serde not found for data type: " + type);
    }
}
