package io.axual.ksml.parser;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 Axual B.V.
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


import io.axual.ksml.dsl.BaseStreamDefinition;
import io.axual.ksml.exception.KSMLParseException;
import io.axual.ksml.operation.MergeOperation;
import io.axual.ksml.stream.KStreamWrapper;

import static io.axual.ksml.dsl.KSMLDSL.MERGE_STREAM_ATTRIBUTE;

public class MergeOperationParser extends ContextAwareParser<MergeOperation> {
    private final String name;

    protected MergeOperationParser(String name, ParseContext context) {
        super(context);
        this.name = name;
    }

    @Override
    public MergeOperation parse(YamlNode node) {
        if (node == null) return null;
        BaseStreamDefinition stream = parseStreamDefinition(node, MERGE_STREAM_ATTRIBUTE, new StreamDefinitionParser());
        if (stream != null) {
            return new MergeOperation(
                    name,
                    context.getStream(stream, KStreamWrapper.class));
        }
        throw new KSMLParseException(node, "Stream not specified");
    }
}
