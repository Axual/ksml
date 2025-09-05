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

import io.axual.ksml.data.mapper.DataObjectMapper;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.serde.StringSerde;
import io.axual.ksml.data.type.DataType;

/**
 * Kafka Serde for JSON using KSML DataObject mapping.
 *
 * <p>JsonSerde extends {@link StringSerde} and plugs in a {@link JsonDataObjectMapper} to
 * handle conversion between JSON text and KSML {@code DataObject}s at the serde boundary.
 * It validates inputs/outputs against an expected {@link DataType}.</p>
 *
 * <p>Usage: constructed by {@link JsonNotation#serde(DataType, boolean)} for supported
 * JSON data types (Struct/Map/List/Union of both).</p>
 */
public class JsonSerde extends StringSerde {
    /** The String-side mapper that handles JSON <-> DataObject mapping. */
    private static final DataObjectMapper<String> STRING_MAPPER = new JsonDataObjectMapper(false);

    /**
     * Creates a JsonSerde for the given expected data type.
     *
     * @param nativeMapper the native-to-DataObject mapper used before reaching the String boundary
     * @param expectedType the expected data type to validate serialized/deserialized values against
     */
    public JsonSerde(NativeDataObjectMapper nativeMapper, DataType expectedType) {
        super(nativeMapper, STRING_MAPPER, expectedType);
    }
}
