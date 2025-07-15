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


import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.GlobalTableDefinition;
import io.axual.ksml.definition.StreamDefinition;
import io.axual.ksml.definition.TableDefinition;
import io.axual.ksml.definition.parser.*;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.exception.ParseException;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.BaseOperation;
import io.axual.ksml.operation.JoinWithGlobalTableOperation;
import io.axual.ksml.operation.JoinWithStreamOperation;
import io.axual.ksml.operation.JoinWithTableOperation;
import io.axual.ksml.parser.ParseNode;
import io.axual.ksml.parser.StructsParser;
import io.axual.ksml.store.StoreType;

import java.util.ArrayList;
import java.util.List;

import static io.axual.ksml.dsl.KSMLDSL.Operations;

public class JoinOperationParser extends OperationParser<BaseOperation> {
    private final StructsParser<JoinWithStreamOperation> joinStreamParser;
    private final StructsParser<JoinWithTableOperation> joinTableParser;
    private final StructsParser<JoinWithGlobalTableOperation> joinGlobalTableParser;
    private final List<StructSchema> schemas = new ArrayList<>();

    public JoinOperationParser(TopologyResources resources) {
        super(KSMLDSL.Operations.JOIN, resources);
        // Set up a common field to prevent warnings about multiple fixed string definitions
        final var valueJoiner = functionField(KSMLDSL.Operations.Join.VALUE_JOINER, "A function that joins two values", new ValueJoinerDefinitionParser(false));

        joinStreamParser = createJoinStreamParser(valueJoiner);
        joinTableParser = createJoinTableParser(valueJoiner);
        joinGlobalTableParser = createJoinGlobalTableParser(valueJoiner);

        schemas.addAll(joinStreamParser.schemas());
        schemas.addAll(joinTableParser.schemas());
        schemas.addAll(joinGlobalTableParser.schemas());
    }

    public StructsParser<BaseOperation> parser() {
        return new StructsParser<>() {
            @Override
            public BaseOperation parse(ParseNode node) {
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
            public List<StructSchema> schemas() {
                return schemas;
            }
        };
    }

    private StructsParser<JoinWithStreamOperation> createJoinStreamParser(StructsParser<FunctionDefinition> valueJoinerField) {
        return structsParser(
                JoinWithStreamOperation.class,
                "",
                "Operation to join with a stream",
                operationNameField(),
                topicField(Operations.Join.WITH_STREAM, "A reference to the stream, or an inline definition of the stream to join with", new StreamDefinitionParser(resources(), true)),
                valueJoinerField,
                durationField(Operations.Join.TIME_DIFFERENCE, "The maximum time difference for a join over two streams on the same key"),
                optional(durationField(Operations.Join.GRACE, "The window grace period (the time to admit out-of-order events after the end of the window)")),
                storeField(Operations.SOURCE_STORE_ATTRIBUTE, true, "Materialized view of the source stream", StoreType.WINDOW_STORE),
                storeField(Operations.OTHER_STORE_ATTRIBUTE, true, "Materialized view of the joined stream", StoreType.WINDOW_STORE),
                (name, stream, valueJoiner, timeDifference, grace, thisStore, otherStore, tags) -> {
                    if (stream instanceof StreamDefinition streamDef) {
                        return new JoinWithStreamOperation(dualStoreOperationConfig(name, tags, thisStore, otherStore), streamDef, valueJoiner, timeDifference, grace);
                    }
                    throw new TopologyException("Join stream not correct, should be a defined stream");
                });
    }

    private StructsParser<JoinWithTableOperation> createJoinTableParser(StructsParser<FunctionDefinition> valueJoinerField) {
        return structsParser(
                JoinWithTableOperation.class,
                "",
                "Operation to join with a table",
                operationNameField(),
                topicField(Operations.Join.WITH_TABLE, "A reference to the table, or an inline definition of the table to join with", new TableDefinitionParser(resources(), true)),
                optional(functionField(Operations.Join.FOREIGN_KEY_EXTRACTOR, "A function that can translate the join table value to a primary key", new ForeignKeyExtractorDefinitionParser(false))),
                valueJoinerField,
                optional(durationField(Operations.Join.GRACE, "The window grace period (the time to admit out-of-order events after the end of the window)")),
                optional(functionField(Operations.Join.PARTITIONER, "A function that partitions the records on the primary table", new StreamPartitionerDefinitionParser(false))),
                optional(functionField(Operations.Join.OTHER_PARTITIONER, "A function that partitions the records on the join table", new StreamPartitionerDefinitionParser(false))),
                storeField(false, "Materialized view of the joined table (only used for Table-Table joins)", StoreType.KEYVALUE_STORE),
                (name, table, foreignKeyExtractor, valueJoiner, grace, partitioner, otherPartitioner, store, tags) -> {
                    if (table instanceof TableDefinition tableDef) {
                        return new JoinWithTableOperation(storeOperationConfig(name, tags, store), tableDef, foreignKeyExtractor, valueJoiner, grace, partitioner, otherPartitioner);
                    }
                    throw new TopologyException("Join table not correct, should be a defined table");
                });
    }

    private StructsParser<JoinWithGlobalTableOperation> createJoinGlobalTableParser(StructsParser<FunctionDefinition> valueJoinerField) {
        return structsParser(
                JoinWithGlobalTableOperation.class,
                "",
                "Operation to join with a table",
                operationNameField(),
                topicField(Operations.Join.WITH_GLOBAL_TABLE, "A reference to the globalTable, or an inline definition of the globalTable to join with", new GlobalTableDefinitionParser(resources(), true)),
                functionField(Operations.Join.MAPPER, "A function that maps the key value from the stream to the primary key type of the globalTable", new KeyValueMapperDefinitionParser(false)),
                valueJoinerField,
                // GlobalTable joins do not use/require a state store
                (name, globalTable, mapper, valueJoiner, tags) -> {
                    if (globalTable instanceof GlobalTableDefinition globalTableDef) {
                        return new JoinWithGlobalTableOperation(operationConfig(name, tags), globalTableDef, mapper, valueJoiner);
                    }
                    throw new TopologyException("Join globalTable not correct, should be a defined globalTable");
                });
    }
}
