package io.axual.ksml.parser.schema;

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

import io.axual.ksml.data.schema.DataField;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.UnionSchema;
import io.axual.ksml.data.value.Pair;
import io.axual.ksml.parser.BaseParser;
import io.axual.ksml.parser.YamlNode;

import java.util.ArrayList;
import java.util.Arrays;

import static io.axual.ksml.dsl.DataSchemaDSL.*;

public class DataFieldParser extends BaseParser<DataField> {
    private static final DataSchemaParser dataSchemaParser = new DataSchemaParser();
    private static final DataSchema NULL_SCHEMA = DataSchema.create(DataSchema.Type.NULL);

    @Override
    public DataField parse(YamlNode node) {
        final var schema = dataSchemaParser.parse(node);
        final var required = parseBoolean(node, DATA_FIELD_REQUIRED_FIELD);
        final var property = deductSchemaAndRequired(schema, required);
        final var constant = parseBoolean(node, DATA_FIELD_CONSTANT_FIELD);
        return new DataField(
                parseString(node, DATA_FIELD_NAME_FIELD),
                property.left(),
                parseString(node, DATA_FIELD_DOC_FIELD),
                property.right(),
                constant != null && constant,
                new DataValueParser().parse(node.get(DATA_FIELD_DEFAULT_VALUE_FIELD)),
                new DataFieldOrderParser().parse(node.get(DATA_FIELD_ORDER_FIELD)));
    }

    private static Pair<DataSchema, Boolean> deductSchemaAndRequired(DataSchema schema, Boolean required) {
        // If we could parse the "required" field from the schema, then return the combination immediately
        if (required != null) return Pair.of(schema, required);

        // We could not parse a required field. In that case, the field is required if the schema is a UnionSchema with
        // a NULL type in its list.
        if (!(schema instanceof UnionSchema unionSchema)) return Pair.of(schema, true);
        final var possibleSchemas = Arrays.stream(unionSchema.possibleSchemas()).toList();

        // If there is no NULL type in the list, then conclude this is a required field
        if (!possibleSchemas.contains(DataSchema.create(DataSchema.Type.NULL))) return Pair.of(schema, true);

        // Create a copy of the union without NULL and return it along with "required=false"
        final var newSchemas = new ArrayList<>(possibleSchemas);
        newSchemas.remove(NULL_SCHEMA);
        return Pair.of(new UnionSchema(newSchemas.toArray(DataSchema[]::new)), false);
    }
}
