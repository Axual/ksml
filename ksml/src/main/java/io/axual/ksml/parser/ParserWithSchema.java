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

import io.axual.ksml.data.schema.DataSchema;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Base interface for a {@link Parser} that has the ability to return a {@link DataSchema} for the type T is returns.
 * @param <T>
 */
public interface ParserWithSchema<T> extends Parser<T> {

    /**
     * The schema that represents the structure of the parsed object.
     * @return the DataSchema for the type T.
     */
    DataSchema schema();

    /**
     * Create a ParserWithSchema from a parse function and a schema.. The caller is responsible for ensuring that the schema is valid for the type T.
     * @param parseFunc a parse function that takes a ParseNode and returns an instance of T.
     * @param schema the schema for the type T.
     * @return a ParserWithSchema that can parse T and return the schema.
     * @param <T> the type of the parsed object.
     */
    static <T> ParserWithSchema<T> of(final Function<ParseNode, T> parseFunc, DataSchema schema) {
        return of(parseFunc, () -> schema);
    }

    /**
     * Create a ParserWithSchema from a parse function and a schema supplier. The caller is responsible for ensuring that the schema is valid for the type T.
     * @param parseFunc a parse function that takes a ParseNode and returns an instance of T.
     * @param getter a supplier that returns the schema for the type T.
     * @return a ParserWithSchema that can parse T and return the schema.
     * @param <T> the type of the parsed object.
     */
    static <T> ParserWithSchema<T> of(final Function<ParseNode, T> parseFunc, Supplier<DataSchema> getter) {
        return new ParserWithSchema<>() {
            @Override
            public T parse(ParseNode node) {
                return parseFunc.apply(node);
            }

            @Override
            public DataSchema schema() {
                return getter.get();
            }
        };
    }
}
