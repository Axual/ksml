package io.axual.ksml.operation.parser;

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


import io.axual.ksml.data.exception.ParseException;
import io.axual.ksml.data.parser.ParseNode;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.definition.StreamDefinition;
import io.axual.ksml.definition.TableDefinition;
import io.axual.ksml.definition.parser.JoinTargetDefinitionParser;
import io.axual.ksml.definition.parser.StreamDefinitionParser;
import io.axual.ksml.definition.parser.TableDefinitionParser;
import io.axual.ksml.definition.parser.ValueJoinerDefinitionParser;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.JoinOperation;
import io.axual.ksml.operation.OuterJoinOperation;
import io.axual.ksml.parser.StructParser;
import io.axual.ksml.util.ListUtil;

public class OuterJoinOperationParser extends StoreOperationParser<OuterJoinOperation> {
    private final StructParser<OuterJoinOperation> joinStreamParser;
    private final StructParser<OuterJoinOperation> joinTableParser;
    private final StructSchema schema;

    public OuterJoinOperationParser(TopologyResources resources) {
        super(KSMLDSL.Operations.OUTER_JOIN, resources);
        joinStreamParser = structParser(
                OuterJoinOperation.class,
                "",
                "Operation to join with a stream",
                operationTypeField(),
                operationNameField(),
                topicField(KSMLDSL.Operations.Join.WITH_STREAM, "(Required for Stream joins) A reference to the Stream, or an inline definition of the Stream to join with", new StreamDefinitionParser(false)),
                functionField(KSMLDSL.Operations.Join.VALUE_JOINER, "(Stream joins) A function that joins two values", new ValueJoinerDefinitionParser()),
                durationField(KSMLDSL.Operations.Join.TIME_DIFFERENCE, "(Stream joins) The maximum time difference for a join over two streams on the same key"),
                optional(durationField(KSMLDSL.Operations.Join.GRACE, "(Stream joins) The window grace period (the time to admit out-of-order events after the end of the window)")),
                storeField(false, "Materialized view of the joined streams", null),
                (type, name, stream, valueJoiner, timeDifference, grace, store, tags) -> {
                    if (stream instanceof StreamDefinition streamDef) {
                        return new OuterJoinOperation(storeOperationConfig(name, tags, store), streamDef, valueJoiner, timeDifference, grace);
                    }
                    throw new TopologyException("Join stream not correct, should be a defined Stream");
                });

        joinTableParser = structParser(
                OuterJoinOperation.class,
                "",
                "Operation to join with a table",
                operationTypeField(),
                operationNameField(),
                topicField(KSMLDSL.Operations.Join.WITH_TABLE, "(Required for Table joins) A reference to the Table, or an inline definition of the Table to join with", new TableDefinitionParser(false)),
                functionField(KSMLDSL.Operations.Join.VALUE_JOINER, "(Table joins) A function that joins two values", new ValueJoinerDefinitionParser()),
                storeField(false, "Materialized view of the joined streams", null),
                (type, name, table, valueJoiner, store, tags) -> {
                    if (table instanceof TableDefinition tableDef) {
                        return new OuterJoinOperation(storeOperationConfig(name, tags, store), tableDef, valueJoiner);
                    }
                    throw new TopologyException("Join table not correct, should be a defined Table");
                });

        schema = structSchema(JoinOperation.class, "Defines a leftJoin operation", ListUtil.union(joinStreamParser.fields(), joinTableParser.fields()));
    }

    public StructParser<OuterJoinOperation> parser() {
        return new StructParser<>() {
            @Override
            public OuterJoinOperation parse(ParseNode node) {
                if (node == null) return null;
                final var joinTopic = new JoinTargetDefinitionParser(resources()).parse(node);
                if (joinTopic.definition() instanceof StreamDefinition) return joinStreamParser.parse(node);
                if (joinTopic.definition() instanceof TableDefinition) return joinTableParser.parse(node);

                final var separator = joinTopic.name() != null && joinTopic.definition() != null ? ", " : "";
                final var description = (joinTopic.name() != null ? joinTopic.name() : "") + separator + (joinTopic.definition() != null ? joinTopic.definition() : "");
                throw new ParseException(node, "Join stream not found: " + description);
            }

            @Override
            public StructSchema schema() {
                return schema;
            }
        };
    }
}
