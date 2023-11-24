package io.axual.ksml.datagenerator.parser;

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


import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.exception.KSMLParseException;
import io.axual.ksml.parser.BaseParser;
import io.axual.ksml.parser.ReferenceOrInlineParser;
import io.axual.ksml.parser.YamlNode;
import io.axual.ksml.user.UserFunction;

public abstract class ContextAwareParser<T> extends BaseParser<T> {
    protected final ParseContext context;

    protected ContextAwareParser(ParseContext context) {
        this.context = context;
    }

    protected <F extends FunctionDefinition> UserFunction parseFunction(YamlNode parent, String childName, BaseParser<F> parser) {
        return parseFunction(parent, childName, parser, false);
    }

    protected <F extends FunctionDefinition> UserFunction parseOptionalFunction(YamlNode parent, String childName, BaseParser<F> parser) {
        if (parent.get(childName) == null) return null;
        return parseFunction(parent, childName, parser, false);
    }

    protected <F extends FunctionDefinition> UserFunction parseFunction(YamlNode parent, String childName, BaseParser<F> parser, boolean allowNull) {
        final var namedDefinition = new ReferenceOrInlineParser<>("function", childName, context.getFunctionDefinitions()::get, parser).parse(parent);
        final var definition = namedDefinition != null ? namedDefinition.definition() : null;
        if (definition == null) {
            if (allowNull) return null;
            throw new KSMLParseException(parent, "User function definition not found, add '" + childName + "' to specification");
        }
        var childNode = parent.appendName(childName);
        var functionName = childNode.getLongName();
        return namedDefinition.name() != null
                ? context.createNamedUserFunction(functionName, definition)
                : context.createAnonUserFunction(functionName, definition, childNode);
    }
}
