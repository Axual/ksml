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

import io.axual.ksml.definition.TableDefinition;
import io.axual.ksml.generator.TopologyBaseResources;
import io.axual.ksml.parser.StructsParser;
import io.axual.ksml.type.UserType;

public class TableDefinitionParser extends BaseTableDefinitionParser<TableDefinition> {
    public TableDefinitionParser(TopologyBaseResources resources, boolean isJoinTarget) {
        super(resources, isJoinTarget, "table");
    }

    @Override
    public StructsParser<TableDefinition> parser() {
        if (!isJoinTarget) return structsParser(
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
                (topic, keyType, valueType, resetPolicy, tsExtractor, partitioner, store, tags) -> {
                    keyType = keyType != null ? keyType : UserType.UNKNOWN;
                    valueType = valueType != null ? valueType : UserType.UNKNOWN;
                    final var policy = OffsetResetPolicyParser.parseResetPolicy(resetPolicy);
                    // If a backing store is used, then align its name, keyType and valueType to the topic
                    return new TableDefinition(topic, keyType, valueType, policy, tsExtractor, partitioner, store != null ? store.with(topic).with(keyType, valueType) : null);
                });

        return structsParser(
                TableDefinition.class,
                "AsJoinTarget",
                "Reference to a " + tableType + " in a join operation",
                topicField(),
                optional(keyField()),
                optional(valueField()),
                partitionerField(),
                storeField(),
                (topic, keyType, valueType, partitioner, store, tags) -> {
                    keyType = keyType != null ? keyType : UserType.UNKNOWN;
                    valueType = valueType != null ? valueType : UserType.UNKNOWN;
                    // If a backing store is used, then align its name, keyType and valueType to the topic
                    return new TableDefinition(topic, keyType, valueType, null, null, partitioner, store != null ? store.with(topic).with(keyType, valueType) : null);
                });
    }
}
