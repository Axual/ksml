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


import io.axual.ksml.parser.FieldParsers;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.definition.StreamDefinition;
import io.axual.ksml.definition.TableDefinition;
import io.axual.ksml.definition.parser.JoinTargetDefinitionParser;
import io.axual.ksml.definition.parser.StreamDefinitionParser;
import io.axual.ksml.definition.parser.TableDefinitionParser;
import io.axual.ksml.definition.parser.ValueJoinerDefinitionParser;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.BaseOperation;
import io.axual.ksml.operation.OuterJoinWithStreamOperation;
import io.axual.ksml.operation.OuterJoinWithTableOperation;
import io.axual.ksml.parser.ParseNode;
import io.axual.ksml.parser.StructsParser;
import io.axual.ksml.store.StoreType;

import java.util.ArrayList;
import java.util.List;

public class OuterJoinOperationParser extends OperationParser<BaseOperation> {
    private final StructsParser<OuterJoinWithStreamOperation> joinStreamParser;
    private final StructsParser<OuterJoinWithTableOperation> joinTableParser;
    private final List<StructSchema> schemas = new ArrayList<>();

    public OuterJoinOperationParser(TopologyResources resources) {
        super(KSMLDSL.Operations.OUTER_JOIN, resources);
        joinStreamParser = createOuterJoinStreamParser();
        joinTableParser = createOuterJoinTableParser();

        schemas.addAll(joinStreamParser.schemas());
        schemas.addAll(joinTableParser.schemas());
    }

    public StructsParser<BaseOperation> parser() {
        return new StructsParser<>() {
            @Override
            public BaseOperation parse(ParseNode node) {
                if (node == null) return null;
                final var joinTopic = new JoinTargetDefinitionParser(resources()).parse(node);
                final var variant = joinVariantOf(joinTopic, node, joinStreamParser, joinTableParser, null);
                if (variant != null) return variant.parse(node);
                throw joinTargetNotFound(node, "OuterJoin", joinTopic);
            }

            @Override
            public List<StructSchema> schemas() {
                return schemas;
            }
        };
    }

    private StructsParser<OuterJoinWithStreamOperation> createOuterJoinStreamParser() {
        return FieldParsers.structsParser(
                OuterJoinWithStreamOperation.class,
                "",
                "Operation to outerJoin with a stream",
                operationNameField(),
                targetField(KSMLDSL.Operations.Join.WITH_STREAM, "A reference to a defined stream or to a stream saved with \"as\" in an earlier pipeline, or an inline definition of the stream to outerJoin with", new StreamDefinitionParser(resources(), true)),
                functionField(KSMLDSL.Operations.Join.VALUE_JOINER, "A function that joins two values", new ValueJoinerDefinitionParser(false)),
                FieldParsers.durationField(KSMLDSL.Operations.Join.TIME_DIFFERENCE, "The maximum time difference for an outerJoin over two streams on the same key"),
                FieldParsers.optional(FieldParsers.durationField(KSMLDSL.Operations.Join.GRACE, "The window grace period (the time to admit out-of-order events after the end of the window)")),
                storeField(KSMLDSL.Operations.SOURCE_STORE_ATTRIBUTE, true, "Materialized view of the source stream", StoreType.WINDOW_STORE),
                storeField(KSMLDSL.Operations.OTHER_STORE_ATTRIBUTE, true, "Materialized view of the outerJoined stream", StoreType.WINDOW_STORE),
                (name, stream, valueJoiner, timeDifference, grace, thisStore, otherStore, tags) -> {
                    return new OuterJoinWithStreamOperation(dualStoreOperationConfig(name, tags, thisStore, otherStore), targetOf(stream, StreamDefinition.class, "OuterJoin stream not correct, should be a defined stream"), valueJoiner, timeDifference, grace);
                });
    }

    private StructsParser<OuterJoinWithTableOperation> createOuterJoinTableParser() {
        return FieldParsers.structsParser(
                OuterJoinWithTableOperation.class,
                "",
                "Operation to outerJoin with a table",
                operationNameField(),
                targetField(KSMLDSL.Operations.Join.WITH_TABLE, "A reference to a defined table or to a table saved with \"as\" in an earlier pipeline, or an inline definition of the table to outerJoin with", new TableDefinitionParser(resources(), true)),
                functionField(KSMLDSL.Operations.Join.VALUE_JOINER, "A function that joins two values", new ValueJoinerDefinitionParser(false)),
                storeField(false, "Materialized view of the outerJoined table", StoreType.KEYVALUE_STORE),
                (name, table, valueJoiner, store, tags) -> {
                    return new OuterJoinWithTableOperation(storeOperationConfig(name, tags, store), targetOf(table, TableDefinition.class, "OuterJoin table not correct, should be a defined table"), valueJoiner);
                });
    }
}
