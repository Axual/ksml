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
import io.axual.ksml.definition.GlobalTableDefinition;
import io.axual.ksml.definition.StreamDefinition;
import io.axual.ksml.definition.TableDefinition;
import io.axual.ksml.definition.parser.*;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.execution.FatalError;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.JoinOperation;
import io.axual.ksml.parser.StructParser;
import io.axual.ksml.util.ListUtil;

import static io.axual.ksml.dsl.KSMLDSL.Operations;

public class JoinOperationParser extends StoreOperationParser<JoinOperation> {
    private final StructParser<JoinOperation> joinStreamParser;
    private final StructParser<JoinOperation> joinTableParser;
    private final StructParser<JoinOperation> joinGlobalTableParser;
    private final StructSchema schema;

    public JoinOperationParser(TopologyResources resources) {
        super("join", resources);
        joinStreamParser = structParser(
                JoinOperation.class,
                "Operation to join with a stream",
                operationTypeField(Operations.JOIN),
                nameField(),
                topicField(Operations.Join.WITH_STREAM, true, "(Required for Stream joins) A reference to the Stream, or an inline definition of the Stream to join with", new StreamDefinitionParser(false)),
                functionField(Operations.Join.VALUE_JOINER, "(Stream joins) A function that joins two values", new ValueJoinerDefinitionParser()),
                durationField(Operations.Join.TIME_DIFFERENCE, true, "(Stream joins) The maximum time difference for a join over two streams on the same key"),
                durationField(Operations.Join.GRACE, false, "(Stream joins) The window grace period (the time to admit out-of-order events after the end of the window)"),
                storeField(false, "Materialized view of the joined streams", null),
                (type, name, stream, valueJoiner, timeDifference, grace, store) -> {
                    if (stream instanceof StreamDefinition streamDef) {
                        return new JoinOperation(storeOperationConfig(name, null, store), streamDef, valueJoiner, timeDifference, grace);
                    }
                    throw FatalError.topologyError("Join stream not correct, should be a defined Stream");
                });

        joinTableParser = structParser(
                JoinOperation.class,
                "Operation to join with a table",
                stringField(KSMLDSL.Operations.TYPE_ATTRIBUTE, true, "The type of the operation, fixed value \"" + Operations.JOIN + "\""),
                nameField(),
                topicField(Operations.Join.WITH_TABLE, true, "(Required for Table joins) A reference to the Table, or an inline definition of the Table to join with", new TableDefinitionParser(false)),
                functionField(Operations.Join.FOREIGN_KEY_EXTRACTOR, "(Table joins) A function that can translate the join table value to a primary key", new ValueJoinerDefinitionParser()),
                functionField(Operations.Join.VALUE_JOINER, "(Table joins) A function that joins two values", new ValueJoinerDefinitionParser()),
                durationField(Operations.Join.GRACE, false, "(Table joins) The window grace period (the time to admit out-of-order events after the end of the window)"),
                functionField(Operations.Join.PARTITIONER, false, "(Table joins) A function that partitions the records on the primary table", new StreamPartitionerDefinitionParser()),
                functionField(Operations.Join.OTHER_PARTITIONER, false, "(Table joins) A function that partitions the records on the join table", new StreamPartitionerDefinitionParser()),
                storeField(false, "Materialized view of the joined streams", null),
                (type, name, table, foreignKeyExtractor, valueJoiner, grace, partitioner, otherPartitioner, store) -> {
                    if (table instanceof TableDefinition tableDef) {
                        return new JoinOperation(storeOperationConfig(name, null, store), tableDef, foreignKeyExtractor, valueJoiner, grace, partitioner, otherPartitioner);
                    }
                    throw FatalError.topologyError("Join table not correct, should be a defined Table");
                });

        joinGlobalTableParser = structParser(
                JoinOperation.class,
                "Operation to join with a table",
                stringField(KSMLDSL.Operations.TYPE_ATTRIBUTE, true, "The type of the operation, fixed value \"" + Operations.JOIN + "\""),
                nameField(),
                topicField(Operations.Join.WITH_GLOBAL_TABLE, true, "(Required for GlobalTable joins) A reference to the GlobalTable, or an inline definition of the GlobalTable to join with", new GlobalTableDefinitionParser(false)),
                functionField(Operations.Join.MAPPER, "(GlobalTable joins) A function that maps the key value from the stream with the primary key of the GlobalTable", new ValueJoinerDefinitionParser()),
                functionField(Operations.Join.VALUE_JOINER, "(GlobalTable joins) A function that joins two values", new ValueJoinerDefinitionParser()),
                storeField(false, "Materialized view of the joined streams", null),
                (type, name, globalTable, mapper, valueJoiner, store) -> {
                    if (globalTable instanceof GlobalTableDefinition globalTableDef) {
                        return new JoinOperation(storeOperationConfig(name, null, store), globalTableDef, mapper, valueJoiner);
                    }
                    throw FatalError.topologyError("Join table not correct, should be a defined Table");
                });

        schema = structSchema(JoinOperation.class, "Defines a join operation", ListUtil.union(joinStreamParser.fields(), ListUtil.union(joinTableParser.fields(), joinGlobalTableParser.fields())));
    }

    public StructParser<JoinOperation> parser() {
        return new StructParser<>() {
            @Override
            public JoinOperation parse(ParseNode node) {
                if (node == null) return null;

                final var joinTopic = new JoinTargetDefinitionParser(resources()).parse(node);
                if (joinTopic.definition() instanceof StreamDefinition) return joinStreamParser.parse(node);
                if (joinTopic.definition() instanceof TableDefinition) return joinTableParser.parse(node);
                if (joinTopic.definition() instanceof GlobalTableDefinition) return joinGlobalTableParser.parse(node);

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
