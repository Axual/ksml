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
import io.axual.ksml.definition.parser.ValueJoinerDefinitionParser;
import io.axual.ksml.definition.parser.WithTopicDefinitionParser;
import io.axual.ksml.exception.KSMLParseException;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.OuterJoinOperation;
import io.axual.ksml.parser.YamlNode;

import static io.axual.ksml.dsl.KSMLDSL.*;

public class OuterJoinOperationParser extends StoreOperationParser<OuterJoinOperation> {
    public OuterJoinOperationParser(String name, TopologyResources resources) {
        super(name, resources);
    }

    @Override
    public OuterJoinOperation parse(YamlNode node) {
        if (node == null) return null;
        final var joinTopic = new WithTopicDefinitionParser(resources).parse(node);
        if (joinTopic instanceof StreamDefinition joinStream) {
            return new OuterJoinOperation(
                    storeOperationConfig(node, STORE_ATTRIBUTE),
                    joinStream,
                    parseFunction(node, JOIN_VALUEJOINER_ATTRIBUTE, new ValueJoinerDefinitionParser()),
                    parseDuration(node, JOIN_WINDOW_ATTRIBUTE));
        }
        throw new KSMLParseException(node, "Stream not specified");
    }
}
