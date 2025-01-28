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
import io.axual.ksml.operation.OuterJoinOperation;
import io.axual.ksml.parser.StructsParser;

import java.util.ArrayList;
import java.util.List;

public class OuterJoinOperationParser extends StoreOperationParser<OuterJoinOperation> {
    private final StructsParser<OuterJoinOperation> joinStreamParser;
    private final StructsParser<OuterJoinOperation> joinTableParser;
    private final List<StructSchema> schemas = new ArrayList<>();

    public OuterJoinOperationParser(TopologyResources resources) {
        super(KSMLDSL.Operations.OUTER_JOIN, resources);
        joinStreamParser = structsParser(
                OuterJoinOperation.class,
                KSMLDSL.Types.WITH_STREAM,
                "Operation to outerJoin with a stream",
                operationNameField(),
                topicField(KSMLDSL.Operations.Join.WITH_STREAM, "A reference to the stream, or an inline definition of the stream to outerJoin with", new StreamDefinitionParser(resources(), true)),
                functionField(KSMLDSL.Operations.Join.VALUE_JOINER, "A function that joins two values", new ValueJoinerDefinitionParser(false)),
                durationField(KSMLDSL.Operations.Join.TIME_DIFFERENCE, "The maximum time difference for an outerJoin over two streams on the same key"),
                optional(durationField(KSMLDSL.Operations.Join.GRACE, "The window grace period (the time to admit out-of-order events after the end of the window)")),
                storeField(false, "Materialized view of the outerJoined streams", null),
                (name, stream, valueJoiner, timeDifference, grace, store, tags) -> {
                    if (stream instanceof StreamDefinition streamDef) {
                        return new OuterJoinOperation(storeOperationConfig(name, tags, store), streamDef, valueJoiner, timeDifference, grace);
                    }
                    throw new TopologyException("OuterJoin stream not correct, should be a defined stream");
                });

        joinTableParser = structsParser(
                OuterJoinOperation.class,
                KSMLDSL.Types.WITH_TABLE,
                "Operation to outerJoin with a table",
                operationNameField(),
                topicField(KSMLDSL.Operations.Join.WITH_TABLE, "A reference to the table, or an inline definition of the table to outerJoin with", new TableDefinitionParser(resources(), true)),
                functionField(KSMLDSL.Operations.Join.VALUE_JOINER, "A function that joins two values", new ValueJoinerDefinitionParser(false)),
                storeField(false, "Materialized view of the outerJoined streams", null),
                (name, table, valueJoiner, store, tags) -> {
                    if (table instanceof TableDefinition tableDef) {
                        return new OuterJoinOperation(storeOperationConfig(name, tags, store), tableDef, valueJoiner);
                    }
                    throw new TopologyException("OuterJoin table not correct, should be a defined table");
                });

        schemas.addAll(joinStreamParser.schemas());
        schemas.addAll(joinTableParser.schemas());
    }

    public StructsParser<OuterJoinOperation> parser() {
        return new StructsParser<>() {
            @Override
            public OuterJoinOperation parse(ParseNode node) {
                if (node == null) return null;
                final var joinTopic = new JoinTargetDefinitionParser(resources()).parse(node);
                if (joinTopic.definition() instanceof StreamDefinition) return joinStreamParser.parse(node);
                if (joinTopic.definition() instanceof TableDefinition) return joinTableParser.parse(node);

                final var separator = joinTopic.name() != null && joinTopic.definition() != null ? ", " : "";
                final var description = (joinTopic.name() != null ? joinTopic.name() : "") + separator + (joinTopic.definition() != null ? joinTopic.definition() : "");
                throw new ParseException(node, "OuterJoin stream not found: " + description);
            }

            @Override
            public List<StructSchema> schemas() {
                return schemas;
            }
        };
    }
}
