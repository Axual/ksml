package io.axual.ksml.parser;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
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

import io.axual.ksml.data.schema.DataField;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.MapSchema;
import io.axual.ksml.data.schema.UnionSchema;
import lombok.Getter;
import lombok.NonNull;

import java.util.Map;

/**
 * A parser that handles Map structures, extending the {@link MapParser} with additional
 * support for schema handling and validation. This parser works with maps where
 * individual values have their schemas defined.
 *
 * @param <T> The type of values in the Map being parsed.
 */
@Getter
public class MapWithSchemaParser<T> extends MapParser<T> implements ParserWithSchema<Map<String, T>> {
    /**
     * The schema that represents the structure of the map. It defines the allowed types
     * for the map's values and possibly contains additional schema rules.
     */
    private final DataSchema schema;

    /**
     * Constructs a MapWithSchemaParser that can parse a Map<String, T> with schemas for its elements.
     *
     * @param childTagKey        The key used to extract child tag information from the map's elements.
     *                           This is typically used to manage the mapping of elements during parsing.
     * @param childTagValuePrefix The prefix used for tag values in elements. This helps identify
     *                            or categorize map value tags.
     * @param valueParser        The parser responsible for handling the individual values in the map.
     *                           It provides schemas for the map's values and supports multi-schema handling.
     */
    public MapWithSchemaParser(String childTagKey, String childTagValuePrefix, @NonNull ParserWithSchemas<T> valueParser) {
        super(childTagKey, childTagValuePrefix, valueParser);
        schema = valueParser.schemas().size() == 1
                ? new MapSchema(valueParser.schemas().getFirst())
                : new MapSchema(new UnionSchema(valueParser.schemas().stream().map(DataField::new).toArray(DataField[]::new)));
    }
}
