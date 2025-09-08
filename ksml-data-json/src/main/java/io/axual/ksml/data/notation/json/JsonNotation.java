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
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.base.BaseNotation;
import io.axual.ksml.data.type.*;
import org.apache.kafka.common.serialization.Serde;

/**
 * JSON notation implementation for KSML.
 *
 * <p>Responsibilities:</p>
 * <ul>
 *   <li>Expose the default JSON data type used by KSML ({@link #DEFAULT_TYPE}), which is a union
 *       of {@link StructType} and {@link ListType} to allow either JSON objects or arrays.</li>
 *   <li>Supply a Kafka {@link Serde} for supported data types: {@link MapType}, {@link ListType},
 *       or any type assignable to {@link #DEFAULT_TYPE} (eg. {@link StructType}).</li>
 *   <li>Provide a converter and schema parser specialized for JSON.</li>
 * </ul>
 *
 * <p>Note: The notation name returned by {@link #name()} comes from the provided {@link NotationContext}.
 * When constructing a JSON notation for generic use, set the context name to {@link #NOTATION_NAME}.</p>
 */
public class JsonNotation extends BaseNotation {
    /** The canonical notation name for JSON. */
    public static final String NOTATION_NAME = "json";

    /**
     * The default JSON data type used by KSML: a union of Struct (object) and List (array).
     * This makes it possible to represent both JSON objects and arrays without prior knowledge.
     */
    public static final DataType DEFAULT_TYPE = new UnionType(
            new UnionType.MemberType(new StructType()),
            new UnionType.MemberType(new ListType()));

    /**
     * Creates a JsonNotation with a given {@link NotationContext}.
     * The filename extension is fixed to ".json".
     */
    public JsonNotation(NotationContext context) {
        // Wire the BaseNotation with the JSON-specific converter and schema loader.
        super(context, ".json", DEFAULT_TYPE, new JsonDataObjectConverter(), new JsonSchemaLoader());
    }

    /**
     * Returns the default JSON data type (Struct|List union).
     */
    @Override
    public DataType defaultType() {
        return DEFAULT_TYPE;
    }

    /**
     * Creates a JSON Serde for the given type when supported.
     * Supported types:
     * - {@link MapType} and {@link ListType}
     * - Any type assignable to {@link #DEFAULT_TYPE}, including {@link StructType}
     *
     * @throws DataException if the provided type is not supported for JSON serialization
     */
    @Override
    public Serde<Object> serde(DataType type, boolean isKey) {
        // JSON types can be Map (or Struct), List, or the union type of both Struct and List.
        // The union coverage is checked via assignability to DEFAULT_TYPE.
        if (type instanceof MapType || type instanceof ListType || JsonNotation.DEFAULT_TYPE.isAssignableFrom(type))
            return new JsonSerde(context().nativeDataObjectMapper(), type);
        // Other types cannot be serialized as JSON.
        throw new DataException("JSON serde not found for data type: " + type);
    }
}
