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

import io.axual.ksml.data.parser.ParserWithSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.value.Pair;
import io.axual.ksml.util.ListUtil;

public class CombinedParser<L, R> extends DefinitionParser<Pair<L, R>> {
    private final ParserWithSchema<L> parser1;
    private final ParserWithSchema<R> parser2;
    private final StructSchema schema;

    public CombinedParser(StructParser<L> parser1, StructParser<R> parser2) {
        this.parser1 = parser1;
        this.parser2 = parser2;
        schema = structSchema((String) null, null, ListUtil.union(parser1.fields(), parser2.fields()));
    }

    @Override
    public StructParser<Pair<L, R>> parser() {
        return StructParser.of(node -> Pair.of(parser1.parse(node), parser2.parse(node)), schema);
    }
}
