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


import io.axual.ksml.definition.parser.ValueJoinerDefinitionParser;
import io.axual.ksml.exception.KSMLParseException;
import io.axual.ksml.operation.OuterJoinOperation;
import io.axual.ksml.parser.ParseContext;
import io.axual.ksml.parser.YamlNode;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;

import static io.axual.ksml.dsl.KSMLDSL.JOIN_VALUEJOINER_ATTRIBUTE;
import static io.axual.ksml.dsl.KSMLDSL.JOIN_WINDOW_ATTRIBUTE;
import static io.axual.ksml.dsl.KSMLDSL.STORE_ATTRIBUTE;

public class OuterJoinOperationParser extends StoreOperationParser<OuterJoinOperation> {
    private final String name;

    public OuterJoinOperationParser(String name, ParseContext context) {
        super(context);
        this.name = name;
    }

    @Override
    public OuterJoinOperation parse(YamlNode node) {
        if (node == null) return null;
        StreamWrapper joinStream = parseAndGetStreamWrapper(node);
        if (joinStream instanceof KStreamWrapper kStreamWrapper) {
            return new OuterJoinOperation(
                    storeOperationConfig(name, node, STORE_ATTRIBUTE),
                    kStreamWrapper,
                    parseFunction(node, JOIN_VALUEJOINER_ATTRIBUTE, new ValueJoinerDefinitionParser()),
                    parseDuration(node, JOIN_WINDOW_ATTRIBUTE));
        }
        throw new KSMLParseException(node, "Stream not specified");
    }
}
