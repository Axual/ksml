package io.axual.ksml.definition.parser;

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


import io.axual.ksml.definition.BaseStreamDefinition;
import io.axual.ksml.exception.KSMLParseException;
import io.axual.ksml.parser.ContextAwareParser;
import io.axual.ksml.parser.ReferenceOrInlineParser;
import io.axual.ksml.parser.ParseContext;
import io.axual.ksml.parser.YamlNode;

import static io.axual.ksml.dsl.KSMLDSL.GLOBALTABLE_DEFINITION;
import static io.axual.ksml.dsl.KSMLDSL.STREAM_DEFINITION;
import static io.axual.ksml.dsl.KSMLDSL.TABLE_DEFINITION;

public class BaseStreamDefinitionParser extends ContextAwareParser<BaseStreamDefinition> {
    public BaseStreamDefinitionParser(ParseContext context) {
        super(context);
    }

    @Override
    public BaseStreamDefinition parse(YamlNode node) {
        if (node == null) return null;
        if (parseString(node, STREAM_DEFINITION) != null) {
            return new ReferenceOrInlineParser<>("stream", STREAM_DEFINITION, context.getStreamDefinitions()::get, new StreamDefinitionParser()).parse(node);
        }
        if (parseString(node, TABLE_DEFINITION) != null) {
            return new ReferenceOrInlineParser<>("table", TABLE_DEFINITION, context.getStreamDefinitions()::get, new TableDefinitionParser(context::registerStore)).parse(node);
        }
        if (parseString(node, GLOBALTABLE_DEFINITION) != null) {
            return new ReferenceOrInlineParser<>("globalTable", GLOBALTABLE_DEFINITION, context.getStreamDefinitions()::get, new GlobalTableDefinitionParser()).parse(node);
        }
        throw new KSMLParseException(node, "Stream definition missing");
    }
}
