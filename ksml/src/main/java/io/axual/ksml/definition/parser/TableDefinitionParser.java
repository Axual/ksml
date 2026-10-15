package io.axual.ksml.definition.parser;

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
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.KeyValueStateStoreDefinition;
import io.axual.ksml.definition.TableDefinition;
import io.axual.ksml.generator.TopologyBaseResources;
import io.axual.ksml.metric.MetricTags;
import io.axual.ksml.parser.StructsParser;
import io.axual.ksml.type.UserType;

public class TableDefinitionParser extends BaseTableDefinitionParser<TableDefinition> {
    public TableDefinitionParser(TopologyBaseResources resources, boolean isJoinTarget) {
        super(resources, isJoinTarget, "table");
    }

    @Override
    public StructsParser<TableDefinition> parser() {
        if (!isJoinTarget) return FieldParsers.structsParser(
                TableDefinition.class,
                "",
                "Contains a definition of a " + tableType + ", which can be referenced by producers and pipelines",
                topicField(),
                keyField(),
                valueField(),
                offsetResetPolicyField(),
                timestampExtractorField(),
                partitionerField(),
                storeField(),
                this::buildDefinition);

        return FieldParsers.structsParser(
                TableDefinition.class,
                "AsJoinTarget",
                "Reference to a " + tableType + " in a join operation",
                topicField(),
                FieldParsers.optional(keyField()),
                FieldParsers.optional(valueField()),
                partitionerField(),
                storeField(),
                this::buildJoinTargetDefinition);
    }

    /**
     * Builds a {@link TableDefinition} from its parsed fields, resolving unset key/value types and
     * aligning a backing store (if any) to the topic's own key/value types.
     */
    private TableDefinition buildDefinition(String topic, UserType keyType, UserType valueType, String resetPolicy,
                                            FunctionDefinition tsExtractor, FunctionDefinition partitioner,
                                            KeyValueStateStoreDefinition store, MetricTags tags) {
        keyType = resolveUserType(keyType != null ? keyType : UserType.UNKNOWN, topic, true);
        valueType = resolveUserType(valueType != null ? valueType : UserType.UNKNOWN, topic, false);
        final var policy = OffsetResetPolicyParser.parseResetPolicy(resetPolicy);
        // If a backing store is used, then align its name, keyType and valueType to the topic
        return new TableDefinition(topic, keyType, valueType, policy, tsExtractor, partitioner, store != null ? store.with(topic).with(keyType, valueType) : null);
    }

    /**
     * Builds a {@link TableDefinition} referencing an existing table as a join target, with no
     * reset policy or timestamp extractor of its own.
     */
    private TableDefinition buildJoinTargetDefinition(String topic, UserType keyType, UserType valueType,
                                                       FunctionDefinition partitioner, KeyValueStateStoreDefinition store, MetricTags tags) {
        keyType = resolveUserType(keyType != null ? keyType : UserType.UNKNOWN, topic, true);
        valueType = resolveUserType(valueType != null ? valueType : UserType.UNKNOWN, topic, false);
        // If a backing store is used, then align its name, keyType and valueType to the topic
        return new TableDefinition(topic, keyType, valueType, null, null, partitioner, store != null ? store.with(topic).with(keyType, valueType) : null);
    }
}
