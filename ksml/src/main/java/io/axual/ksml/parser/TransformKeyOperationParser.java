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



import io.axual.ksml.operation.TransformKeyOperation;

import static io.axual.ksml.dsl.KSMLDSL.TRANSFORMKEY_MAPPER_ATTRIBUTE;
import static io.axual.ksml.dsl.KSMLDSL.TRANSFORMKEY_NAME_ATTRIBUTE;

public class TransformKeyOperationParser extends ContextAwareParser<TransformKeyOperation> {
    protected TransformKeyOperationParser(ParseContext context) {
        super(context);
    }

    @Override
    public TransformKeyOperation parse(YamlNode node) {
        if (node == null) return null;
        return new TransformKeyOperation(
                parseFunction(node, TRANSFORMKEY_MAPPER_ATTRIBUTE, new KeyTransformerDefinitionParser()),
                parseText(node, TRANSFORMKEY_NAME_ATTRIBUTE));
    }
}
