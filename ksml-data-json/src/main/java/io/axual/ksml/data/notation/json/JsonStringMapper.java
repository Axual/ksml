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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.notation.string.StringMapper;
import io.axual.ksml.data.util.JsonNodeUtil;

import java.io.IOException;
import java.io.StringWriter;

/**
 * Maps between JSON text and native Java objects using Jackson.
 *
 * <p>This class is used as the String boundary for JSON-based notations. It converts
 * String input to a native object graph (Map/List/primitives) and vice versa. The
 * actual mapping between native graphs and KSML DataObjects is handled elsewhere by
 * {@link io.axual.ksml.data.mapper.NativeDataObjectMapper}.</p>
 *
 * <p>Behavioral notes:</p>
 * <ul>
 *   <li>Null-safe: passing {@code null} returns {@code null} in both directions.</li>
 *   <li>Parsing errors throw {@link DataException} with a brief message.</li>
 *   <li>When {@code prettyPrint} is enabled, output JSON is formatted with whitespace
 *       but remains semantically identical.</li>
 * </ul>
 */
public class JsonStringMapper implements StringMapper<Object> {
    /** Shared Jackson ObjectMapper for parsing and generating JSON. */
    private static final ObjectMapper MAPPER = new ObjectMapper();
    /** Whether to pretty-print output JSON produced by {@link #toString(Object)}. */
    private final boolean prettyPrint;

    /**
     * Creates a JSON String mapper.
     *
     * @param prettyPrint if true, {@link #toString(Object)} produces formatted JSON
     */
    public JsonStringMapper(boolean prettyPrint) {
        this.prettyPrint = prettyPrint;
    }

    /**
     * Parses a JSON string into a native Java object graph.
     *
     * @param value JSON text or {@code null}
     * @return a native representation (Map/List/primitives), or {@code null} if input was {@code null}
     * @throws DataException when the input can not be parsed as JSON
     */
    @Override
    public Object fromString(String value) {
        if (value == null) return null; // Allow null strings as input, returning null as native output
        try {
            var tree = MAPPER.readTree(value);
            return JsonNodeUtil.convertJsonNodeToNative(tree);
        } catch (Exception mapException) {
            // Keep message compact to avoid logging the full value in noisy environments
            throw new DataException("Could not parse string to object: " + value);
        }
    }

    /**
     * Serializes a native Java object graph into a JSON string.
     *
     * @param value a native representation (Map/List/primitives), or {@code null}
     * @return JSON text or {@code null} if input was {@code null}
     * @throws DataException when the value can not be converted to JSON
     */
    @Override
    public String toString(Object value) {
        if (value == null) return null; // Allow null as native input, return null string as output
        try {
            final var writer = new StringWriter();
            final var generator = prettyPrint
                    ? MAPPER.writerWithDefaultPrettyPrinter().createGenerator(writer)
                    : MAPPER.createGenerator(writer);
            MAPPER.writeTree(generator, JsonNodeUtil.convertNativeToJsonNode(value));
            return writer.toString();
        } catch (IOException e) {
            throw new DataException("Can not convert object to JSON string: " + value, e);
        }
    }
}
