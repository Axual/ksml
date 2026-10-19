package io.axual.ksml.operation.parser;

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

import io.axual.ksml.definition.GlobalTableDefinition;
import io.axual.ksml.definition.StreamDefinition;
import io.axual.ksml.definition.TableDefinition;
import io.axual.ksml.definition.TopicDefinition;
import io.axual.ksml.definition.TopologyResource;
import io.axual.ksml.definition.parser.JoinTargetDefinitionParser;
import io.axual.ksml.exception.ParseException;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.BaseOperation;
import io.axual.ksml.parser.ParseNode;
import io.axual.ksml.parser.StructsParser;

import static io.axual.ksml.dsl.KSMLDSL.Operations;

// Shared parsing of join, leftJoin and outerJoin, which choose a variant by the stream, table or globalTable to join with
public abstract class BaseJoinOperationParser extends OperationParser<BaseOperation> {
    protected BaseJoinOperationParser(String type, TopologyResources resources) {
        super(type, resources);
    }

    // For joins without a globalTable variant
    protected BaseOperation parseJoin(ParseNode node, String operation,
                                      StructsParser<? extends BaseOperation> streamParser,
                                      StructsParser<? extends BaseOperation> tableParser) {
        return parseJoin(node, operation, streamParser, tableParser, null);
    }

    protected BaseOperation parseJoin(ParseNode node, String operation,
                                      StructsParser<? extends BaseOperation> streamParser,
                                      StructsParser<? extends BaseOperation> tableParser,
                                      StructsParser<? extends BaseOperation> globalTableParser) {
        if (node == null) return null;
        final var target = new JoinTargetDefinitionParser(resources()).parse(node);
        final var variant = variantOf(target, node, streamParser, tableParser, globalTableParser);
        if (variant != null) return variant.parse(node);
        final var separator = target.name() != null && target.definition() != null ? ", " : "";
        final var description = (target.name() != null ? target.name() : "") + separator + (target.definition() != null ? target.definition() : "");
        throw new ParseException(node, operation + " stream not found: " + description);
    }

    // By definition type, or by key for an "as" name. Null if none fits
    private static StructsParser<? extends BaseOperation> variantOf(TopologyResource<TopicDefinition> target, ParseNode node,
                                                                    StructsParser<? extends BaseOperation> streamParser,
                                                                    StructsParser<? extends BaseOperation> tableParser,
                                                                    StructsParser<? extends BaseOperation> globalTableParser) {
        final var definition = target.definition();
        if (definition instanceof StreamDefinition) return streamParser;
        if (definition instanceof TableDefinition) return tableParser;
        if (definition instanceof GlobalTableDefinition) return globalTableParser;
        if (definition != null) return null;
        final var key = JoinTargetDefinitionParser.keyOf(node);
        if (Operations.Join.WITH_STREAM.equals(key)) return streamParser;
        if (Operations.Join.WITH_TABLE.equals(key)) return tableParser;
        return null;
    }
}
