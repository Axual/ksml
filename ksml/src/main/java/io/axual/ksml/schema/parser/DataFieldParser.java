package io.axual.ksml.schema.parser;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
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
import io.axual.ksml.parser.BaseParser;
import io.axual.ksml.parser.ParseNode;
import io.axual.ksml.util.Pair;

import java.util.Arrays;

import static io.axual.ksml.data.schema.DataSchemaConstants.NO_TAG;

/**
 * The {@code DataFieldParser} class is responsible for parsing a {@link ParseNode}
 * and constructing a {@link DataField} object. This includes determining the field's
 * schema, required status, default value, and other metadata associated with the field.
 * <p>
 * This class ensures that fields adhere to their schema definitions and provides
 * flexibility for handling fields with optional values by evaluating union schemas
 * containing a NULL type.
 */
public class DataFieldParser extends BaseParser<DataField> {
    private static final DataSchemaParser dataSchemaParser = new DataSchemaParser();

    /**
     * Parses a {@link ParseNode} to create a {@link DataField} object.
     * <p>
     * This method extracts various components of a `ParseNode` to construct a `DataField`,
     * including its schema, required status, default value, and other properties.
     *
     * @param node The {@link ParseNode} representation of the field to be parsed.
     * @return A fully constructed {@link DataField} instance based on the input node.
     */
    @Override
    public DataField parse(ParseNode node) {
        final var schema = dataSchemaParser.parse(node);
        final var required = parseBoolean(node, DataSchemaDSL.DATA_FIELD_REQUIRED_FIELD);
        final var property = deduceSchemaAndRequired(schema, required);
        final var constant = parseBoolean(node, DataSchemaDSL.DATA_FIELD_CONSTANT_FIELD);
        final var index = parseInteger(node, DataSchemaDSL.DATA_FIELD_TAG_FIELD);
        return new DataField(
                parseString(node, DataSchemaDSL.DATA_FIELD_NAME_FIELD),
                property.left(),
                parseString(node, DataSchemaDSL.DATA_FIELD_DOC_FIELD),
                index != null ? index : NO_TAG,
                property.right(),
                constant != null && constant,
                new DataValueParser().parse(node.get(DataSchemaDSL.DATA_FIELD_DEFAULT_VALUE_FIELD)),
                new DataFieldOrderParser().parse(node.get(DataSchemaDSL.DATA_FIELD_ORDER_FIELD)));
    }

    /**
     * Derives the schema and required status for a field.
     * <p>
     * This method parses the schema and determines whether the field is required based on
     * the schema type. Specifically, if the schema is a {@link UnionSchema} and contains
     * a `NULL` type, the field is optional; otherwise, it is required.
     *
     * @param schema   The schema associated with the field.
     * @param required The explicitly provided required status (can be null).
     * @return A {@link Pair} containing:
     * - The effective {@link DataSchema} (possibly adjusted if optional).
     * - The required status as a {@code Boolean}.
     */
    private static Pair<DataSchema, Boolean> deduceSchemaAndRequired(DataSchema schema, Boolean required) {
        // If "required" is explicitly provided, use it directly.
        if (required != null) return Pair.of(schema, required);

        // If schema is not a UnionSchema, assume the field is required.
        if (!(schema instanceof UnionSchema unionSchema)) return Pair.of(schema, true);

        // Check whether the UnionSchema contains a NULL type (indicating optionality). Return
        // that the field is required if no NULL was found.
        if (!unionSchema.contains(DataSchema.NULL_SCHEMA)) return Pair.of(schema, true);

        // Create a copy of the UnionSchema excluding NULL types, and mark the field as optional.
        final var newMembers = Arrays.asList(unionSchema.members());
        newMembers.removeIf(field -> field.schema() == DataSchema.NULL_SCHEMA);
        return Pair.of(new UnionSchema(newMembers.toArray(UnionSchema.Member[]::new)), false);
    }
}
