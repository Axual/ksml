package io.axual.ksml.schema.parser;

/*-
 * ========================LICENSE_START=================================
 * KSML
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

import io.axual.ksml.data.schema.UnionSchema;
import io.axual.ksml.parser.BaseParser;
import io.axual.ksml.parser.ParseNode;

import static io.axual.ksml.data.schema.DataSchemaConstants.NO_TAG;

public class UnionMemberParser extends BaseParser<UnionSchema.Member> {
    /**
     * Parses a {@link ParseNode} to create a {@link UnionSchema.Member} object.
     * <p>
     * This method extracts various components of a `ParseNode` to construct a `UnionSchema.Member`,
     * including its schema, and other properties.
     *
     * @param node The {@link ParseNode} representation of the field to be parsed.
     * @return A fully constructed {@link UnionSchema.Member} instance based on the input node.
     */
    @Override
    public UnionSchema.Member parse(ParseNode node) {
        final var tag = parseInteger(node, DataSchemaDSL.UNION_MEMBER_TAG_FIELD);
        return new UnionSchema.Member(
                parseString(node, DataSchemaDSL.UNION_MEMBER_NAME_FIELD),
                new DataSchemaParser().parse(node),
                parseString(node, DataSchemaDSL.UNION_MEMBER_DOC_FIELD),
                tag != null ? tag : NO_TAG);
    }
}
