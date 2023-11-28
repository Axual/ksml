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


import io.axual.ksml.definition.TypedRef;
import io.axual.ksml.exception.KSMLParseException;
import io.axual.ksml.parser.BaseParser;
import io.axual.ksml.parser.ReferenceOrInlineDefinitionParser;
import io.axual.ksml.parser.YamlNode;

import static io.axual.ksml.dsl.KSMLDSL.*;

public class WithTopicDefinitionParser extends BaseParser<TypedRef> {
    @Override
    public TypedRef parse(YamlNode node) {
        if (node == null) return null;
        if (parseString(node, WITH_STREAM_DEFINITION) != null) {
            return new TypedRef(
                    TypedRef.TopicType.STREAM,
                    new ReferenceOrInlineDefinitionParser<>("stream", WITH_STREAM_DEFINITION, new StreamDefinitionParser()).parse(node));
        }
        if (parseString(node, WITH_TABLE_DEFINITION) != null) {
            return new TypedRef(
                    TypedRef.TopicType.TABLE,
                    new ReferenceOrInlineDefinitionParser<>("table", WITH_TABLE_DEFINITION, new TableDefinitionParser()).parse(node));
        }
        if (parseString(node, WITH_GLOBALTABLE_DEFINITION) != null) {
            return new TypedRef(
                    TypedRef.TopicType.GLOBALTABLE,
                    new ReferenceOrInlineDefinitionParser<>("globalTable", WITH_GLOBALTABLE_DEFINITION, new GlobalTableDefinitionParser()).parse(node));
        }
        throw new KSMLParseException(node, "Stream definition missing");
    }
}
