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

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public interface ParserWithSchemas<T> extends Parser<T> {
    List<? extends DataSchema> schemas();

    static <T> ParserWithSchemas<T> of(final Function<ParseNode, T> parseFunc, DataSchema schema) {
        return of(parseFunc, List.of(schema));
    }

    static <T> ParserWithSchemas<T> of(final Function<ParseNode, T> parseFunc, List<DataSchema> schemas) {
        return of(parseFunc, () -> schemas);
    }

    static <T> ParserWithSchemas<T> of(final Function<ParseNode, T> parseFunc, Supplier<List<DataSchema>> getter) {
        return new ParserWithSchemas<>() {
            @Override
            public T parse(ParseNode node) {
                return parseFunc.apply(node);
            }

            @Override
            public List<DataSchema> schemas() {
                return getter.get();
            }
        };
    }
}
