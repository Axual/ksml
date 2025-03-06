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
import io.axual.ksml.data.schema.ListSchema;
import io.axual.ksml.data.schema.UnionSchema;
import lombok.Getter;

import java.util.List;

@Getter
public class ListWithSchemaParser<T> extends ListParser<T> implements ParserWithSchema<List<T>> {
    private final DataSchema schema;

    public ListWithSchemaParser(String childTagKey, String childTagValuePrefix, ParserWithSchema<T> valueParser) {
        super(childTagKey, childTagValuePrefix, valueParser);
        schema = new ListSchema(valueParser.schema());
    }

    public ListWithSchemaParser(String childTagKey, String childTagValuePrefix, ParserWithSchemas<T> valueParser) {
        super(childTagKey, childTagValuePrefix, valueParser);
        schema = valueParser.schemas().size() == 1
                ? new ListSchema(valueParser.schemas().getFirst())
                : new ListSchema(new UnionSchema(valueParser.schemas().stream().map(DataField::new).toArray(DataField[]::new)));
    }
}
