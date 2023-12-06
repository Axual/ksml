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


import io.axual.ksml.definition.StreamDefinition;
import io.axual.ksml.definition.TableDefinition;
import io.axual.ksml.definition.TopicDefinition;
import io.axual.ksml.definition.parser.ValueJoinerDefinitionParser;
import io.axual.ksml.definition.parser.JoinTargetDefinitionParser;
import io.axual.ksml.exception.KSMLParseException;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.LeftJoinOperation;
import io.axual.ksml.parser.YamlNode;

import static io.axual.ksml.dsl.KSMLDSL.*;

public class LeftJoinOperationParser extends StoreOperationParser<LeftJoinOperation> {
    public LeftJoinOperationParser(String prefix, String name, TopologyResources resources) {
        super(prefix, name, resources);
    }

    @Override
    public LeftJoinOperation parse(YamlNode node) {
        if (node == null) return null;
        TopicDefinition joinTopic = new JoinTargetDefinitionParser(prefix, resources).parse(node);
        if (joinTopic instanceof StreamDefinition joinStream) {
            return new LeftJoinOperation(
                    storeOperationConfig(node, STORE_ATTRIBUTE, null),
                    joinStream,
                    parseFunction(node, JOIN_VALUEJOINER_ATTRIBUTE, new ValueJoinerDefinitionParser()),
                    parseDuration(node, JOIN_WINDOW_TIME_DIFFERENCE_ATTRIBUTE),
                    parseDuration(node, JOIN_WINDOW_GRACE_ATTRIBUTE));
        }
        if (joinTopic instanceof TableDefinition joinTable) {
            return new LeftJoinOperation(
                    storeOperationConfig(node, STORE_ATTRIBUTE, null),
                    joinTable,
                    parseFunction(node, JOIN_VALUEJOINER_ATTRIBUTE, new ValueJoinerDefinitionParser()),
                    parseDuration(node, JOIN_WINDOW_GRACE_ATTRIBUTE));
        }

        throw new KSMLParseException(node, "Incorrect join stream type: " + joinTopic.getClass().getSimpleName());
    }
}
