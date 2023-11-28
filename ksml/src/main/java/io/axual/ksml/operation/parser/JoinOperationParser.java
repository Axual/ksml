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


import io.axual.ksml.definition.TypedRef;
import io.axual.ksml.definition.parser.KeyTransformerDefinitionParser;
import io.axual.ksml.definition.parser.ValueJoinerDefinitionParser;
import io.axual.ksml.definition.parser.WithTopicDefinitionParser;
import io.axual.ksml.exception.KSMLParseException;
import io.axual.ksml.operation.JoinOperation;
import io.axual.ksml.parser.YamlNode;

import static io.axual.ksml.dsl.KSMLDSL.*;

public class JoinOperationParser extends StoreOperationParser<JoinOperation> {
    private final String name;

    public JoinOperationParser(String name) {
        this.name = name;
    }

    @Override
    public JoinOperation parse(YamlNode node) {
        if (node == null) return null;
        TypedRef joinStream = new WithTopicDefinitionParser().parse(node);
        switch (joinStream.type()) {
            case STREAM -> new JoinOperation(
                    storeOperationConfig(name, node, STORE_ATTRIBUTE),
                    joinStream,
                    parseFunction(node, JOIN_VALUEJOINER_ATTRIBUTE, new ValueJoinerDefinitionParser()),
                    parseDuration(node, JOIN_WINDOW_ATTRIBUTE));
            case TABLE -> new JoinOperation(
                    storeOperationConfig(name, node, STORE_ATTRIBUTE),
                    joinStream,
                    parseFunction(node, JOIN_VALUEJOINER_ATTRIBUTE, new ValueJoinerDefinitionParser()));
            case GLOBALTABLE -> new JoinOperation(
                    storeOperationConfig(name, node, STORE_ATTRIBUTE),
                    joinStream,
                    parseFunction(node, JOIN_MAPPER_ATTRIBUTE, new KeyTransformerDefinitionParser()),
                    parseFunction(node, JOIN_VALUEJOINER_ATTRIBUTE, new ValueJoinerDefinitionParser()));
        }
        throw new KSMLParseException(node, "Stream not specified");
    }
}
