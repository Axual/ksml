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

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Base interface for parsers that can return a list of {@link DataSchema}s. This is used for parsers that return a list of values, such as {@link ListParser} and {@link MapParser}.
 * @param <T>
 */
public interface ParserWithSchemas<T> extends Parser<T> {

    @SuppressWarnings("java:S1452") // wildcard is required: implementations return List<StructSchema>, a covariant subtype
    List<? extends DataSchema> schemas();

    /**
     * Create a ParserWithSchemas from a parse function and a schema. The caller is responsible for ensuring that the schema is valid for the type T.
     * @param parseFunc a parse function that takes a ParseNode and returns an instance of T.
     * @param schema the schema for the type T.
     * @return a ParserWithSchemas that can parse T and return the schema.
     * @param <T> the type of the parsed object.
     */
    static <T> ParserWithSchemas<T> of(final Function<ParseNode, T> parseFunc, DataSchema schema) {
        return of(parseFunc, List.of(schema));
    }

    /**
     * Create a ParserWithSchemas from a parse function and a list of DataSchemas. The caller is responsible for ensuring that the schema is valid for the type T.
     * @param parseFunc a parse function that takes a ParseNode and returns an instance of T.
     * @param schemas the list of schemas for the type T.
     * @return a ParserWithSchemas that can parse T and return the list of schemas.
     * @param <T> the type of the parsed object.
     */
    static <T> ParserWithSchemas<T> of(final Function<ParseNode, T> parseFunc, List<DataSchema> schemas) {
        return of(parseFunc, () -> schemas);
    }

    /**
     * Create a ParserWithSchemas from a parse function and a supplier for a list of schemas. The caller is responsible for ensuring that the schemas are valid for the type T.
     * @param parseFunc a parse function that takes a ParseNode and returns an instance of T.
     * @param getter a supplier that returns the list of schemas for the type T.
     * @return a ParserWithSchemas that can parse T and return the list of schemas.
     * @param <T> the type of the parsed object.
     */
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
