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



import java.util.concurrent.atomic.AtomicInteger;

import io.axual.ksml.dsl.BaseStreamDefinition;
import io.axual.ksml.dsl.FunctionDefinition;
import io.axual.ksml.exception.KSMLParseException;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserFunction;

public abstract class ContextAwareParser<T> extends BaseParser<T> {
    protected final ParseContext context;

    public ContextAwareParser(ParseContext context) {
        this.context = context;
    }

    protected <F extends FunctionDefinition> UserFunction parseFunction(YamlNode parent, String childName, BaseParser<F> parser) {
        return parseFunction(parent, childName, parser, false);
    }

    protected <F extends FunctionDefinition> UserFunction parseFunction(YamlNode parent, String childName, BaseParser<F> parser, boolean allowNull) {
        FunctionDefinition definition = new InlineOrReferenceParser<>(context.getFunctions(), parser, childName).parse(parent);
        if (allowNull || definition != null) {
            UserFunction result = definition != null ? context.getFunction(definition, parent.appendName(childName).getLongName()) : null;
            if (allowNull || result != null) {
                return result;
            }
            throw new KSMLParseException(parent, "Specified function not found");
        }
        throw new KSMLParseException(parent, "User function definition not found, add '" + childName + "' to specification");
    }

    protected <S extends BaseStreamDefinition> BaseStreamDefinition parseStreamDefinition(YamlNode parent, String childName, BaseParser<S> parser) {
        return new InlineOrReferenceParser<>(context.getStreams(), parser, childName).parse(parent);
    }

    protected BaseStreamDefinition parseStreamDefinition(YamlNode parent, String childName) {
        return parseStreamDefinition(parent, childName, new BaseStreamDefinitionParser(context));
    }

    public StreamWrapper getStream(BaseStreamDefinition definition) {
        return definition != null ? context.getStream(definition) : null;
    }

    protected StreamWrapper parseStream(YamlNode parent) {
        BaseStreamDefinition definition = new BaseStreamDefinitionParser(context).parse(parent);
        return definition != null ? context.getStream(definition) : null;
    }

    protected String determineName(String name, String type) {
        if (name == null || name.trim().isEmpty()) {
            return determineName(type);
        }
        return name.trim();
    }
    
    protected String determineName(String type) {
        return String.format("%s_%03d", type, context.getTypeInstanceCounters().computeIfAbsent(type, t -> new AtomicInteger(1)).getAndIncrement());
    }

}
