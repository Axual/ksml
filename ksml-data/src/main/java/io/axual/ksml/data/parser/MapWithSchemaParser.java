package io.axual.ksml.data.parser;

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

import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.MapSchema;
import lombok.Getter;

import java.util.Map;

@Getter
public class MapWithSchemaParser<T> extends MapParser<T> implements ParserWithSchema<Map<String, T>> {
    private final DataSchema schema;

    public MapWithSchemaParser(String whatToParse, ParserWithSchema<T> valueParser) {
        super(whatToParse, valueParser);
        schema = new MapSchema(valueParser.schema());
    }
}
