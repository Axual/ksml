package io.axual.ksml.data.parser.schema;

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

import io.axual.ksml.data.parser.BaseParser;
import io.axual.ksml.data.parser.ParseNode;
import io.axual.ksml.data.schema.DataField;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.UnionSchema;
import io.axual.ksml.data.value.Pair;

import java.util.ArrayList;
import java.util.Arrays;

import static io.axual.ksml.data.schema.DataField.NO_INDEX;

public class DataFieldParser extends BaseParser<DataField> {
    private static final DataSchemaParser dataSchemaParser = new DataSchemaParser();
    private static final DataSchema NULL_SCHEMA = DataSchema.create(DataSchema.Type.NULL);

    @Override
    public DataField parse(ParseNode node) {
        final var schema = dataSchemaParser.parse(node);
        final var required = parseBoolean(node, DataSchemaDSL.DATA_FIELD_REQUIRED_FIELD);
        final var property = deductSchemaAndRequired(schema, required);
        final var constant = parseBoolean(node, DataSchemaDSL.DATA_FIELD_CONSTANT_FIELD);
        final var index = parseInteger(node, DataSchemaDSL.DATA_FIELD_INDEX_FIELD);
        return new DataField(
                parseString(node, DataSchemaDSL.DATA_FIELD_NAME_FIELD),
                property.left(),
                parseString(node, DataSchemaDSL.DATA_FIELD_DOC_FIELD),
                index != null ? index : NO_INDEX,
                property.right(),
                constant != null && constant,
                new DataValueParser().parse(node.get(DataSchemaDSL.DATA_FIELD_DEFAULT_VALUE_FIELD)),
                new DataFieldOrderParser().parse(node.get(DataSchemaDSL.DATA_FIELD_ORDER_FIELD)));
    }

    private static Pair<DataSchema, Boolean> deductSchemaAndRequired(DataSchema schema, Boolean required) {
        // If we could parse the "required" field from the schema, then return the combination immediately
        if (required != null) return Pair.of(schema, required);

        // We could not parse a required field. In that case, the field is required if the schema is a UnionSchema with
        // a NULL type in its list.
        if (!(schema instanceof UnionSchema unionSchema)) return Pair.of(schema, true);
        final var valueTypes = Arrays.stream(unionSchema.valueTypes()).toList();

        // If there is no NULL type in the list, then conclude this is a required field
        for (final var valueType : valueTypes) {
            if (valueType.schema().type() == DataSchema.Type.NULL) return Pair.of(schema, true);
        }

        // Create a copy of the union without NULL and return it along with "required=false"
        final var newFields = new ArrayList<>(valueTypes);
        newFields.removeIf(field -> field.schema().type() == DataSchema.Type.NULL);
        return Pair.of(new UnionSchema(newFields.toArray(DataField[]::new)), false);
    }
}
