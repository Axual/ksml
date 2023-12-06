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


import io.axual.ksml.definition.GlobalTableDefinition;
import io.axual.ksml.definition.StreamDefinition;
import io.axual.ksml.definition.TableDefinition;
import io.axual.ksml.definition.parser.ForeignKeyExtractorDefinitionParser;
import io.axual.ksml.definition.parser.JoinTargetDefinitionParser;
import io.axual.ksml.definition.parser.KeyTransformerDefinitionParser;
import io.axual.ksml.definition.parser.StreamPartitionerDefinitionParser;
import io.axual.ksml.definition.parser.ValueJoinerDefinitionParser;
import io.axual.ksml.exception.KSMLParseException;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.JoinOperation;
import io.axual.ksml.parser.YamlNode;

import static io.axual.ksml.dsl.KSMLDSL.Operations;

public class JoinOperationParser extends StoreOperationParser<JoinOperation> {
    public JoinOperationParser(String prefix, String name, TopologyResources resources) {
        super(prefix, name, resources);
    }

    @Override
    public JoinOperation parse(YamlNode node) {
        if (node == null) return null;
        final var joinTopic = new JoinTargetDefinitionParser(prefix, resources).parse(node);
        if (joinTopic instanceof StreamDefinition joinStream) {
            return new JoinOperation(
                    storeOperationConfig(node, Operations.STORE_ATTRIBUTE, null),
                    joinStream,
                    parseFunction(node, Operations.Join.VALUE_JOINER, new ValueJoinerDefinitionParser()),
                    parseDuration(node, Operations.Join.TIME_DIFFERENCE),
                    parseDuration(node, Operations.Join.GRACE));
        }
        if (joinTopic instanceof TableDefinition joinTable) {
            return new JoinOperation(
                    storeOperationConfig(node, Operations.STORE_ATTRIBUTE, null),
                    joinTable,
                    parseFunction(node, Operations.Join.FOREIGN_KEY_EXTRACTOR, new ForeignKeyExtractorDefinitionParser()),
                    parseFunction(node, Operations.Join.VALUE_JOINER, new ValueJoinerDefinitionParser()),
                    parseDuration(node, Operations.Join.GRACE),
                    parseFunction(node, Operations.Join.PARTITIONER, new StreamPartitionerDefinitionParser()),
                    parseFunction(node, Operations.Join.OTHER_PARTITIONER, new StreamPartitionerDefinitionParser()));
        }
        if (joinTopic instanceof GlobalTableDefinition globalTableDefinition) {
            return new JoinOperation(
                    storeOperationConfig(node, Operations.STORE_ATTRIBUTE, null),
                    globalTableDefinition,
                    parseFunction(node, Operations.Join.MAPPER, new KeyTransformerDefinitionParser()),
                    parseFunction(node, Operations.Join.VALUE_JOINER, new ValueJoinerDefinitionParser()));
        }
        throw new KSMLParseException(node, "Join stream not specified");
    }
}
