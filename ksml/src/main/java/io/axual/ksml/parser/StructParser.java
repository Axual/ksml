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

import io.axual.ksml.data.parser.ParseNode;
import io.axual.ksml.data.parser.ParserWithSchema;
import io.axual.ksml.data.schema.DataField;
import io.axual.ksml.data.schema.StructSchema;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public interface StructParser<T> extends ParserWithSchema<T> {
    StructSchema schema();

    default List<DataField> fields() {
        return schema().fields();
    }

    static <T> StructParser<T> of(final Function<ParseNode, T> parseFunc, StructSchema schema) {
        return of(parseFunc, () -> schema);
    }

    static <T> StructParser<T> of(final Function<ParseNode, T> parseFunc, Supplier<StructSchema> getter) {
        return new StructParser<T>() {
            @Override
            public T parse(ParseNode node) {
                return parseFunc.apply(node);
            }

            @Override
            public StructSchema schema() {
                return getter.get();
            }
        };
    }
}
