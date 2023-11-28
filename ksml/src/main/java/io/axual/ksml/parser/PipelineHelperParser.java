package io.axual.ksml.parser;

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


import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.Ref;
import io.axual.ksml.definition.StreamDefinition;
import io.axual.ksml.definition.TopicDefinition;
import io.axual.ksml.definition.parser.WithTopicDefinitionParser;
import io.axual.ksml.exception.KSMLParseException;
import io.axual.ksml.execution.FatalError;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserFunction;

import java.util.concurrent.atomic.AtomicInteger;

public abstract class PipelineHelperParser<T> extends BaseParser<T> {
    protected <F extends FunctionDefinition> Ref<FunctionDefinition> parseFunction(YamlNode parent, String childName, BaseParser<F> parser, boolean allowNull) {
        final var namedDefinition = new ReferenceOrInlineDefinitionParser<FunctionDefinition,F>("function", childName, parser).parse(parent);
        final var childNode = parent.appendName(childName);
        final var functionName = childNode.getLongName();
        if (namedDefinition == null || namedDefinition.definition() == null) {
            if (allowNull) return null;
            throw new KSMLParseException(parent, "Could not generate UserFunction for given definition: " + functionName);
        }
        return namedDefinition;
    }

    protected <F extends FunctionDefinition> Ref<FunctionDefinition> parseFunction(YamlNode parent, String childName, BaseParser<F> parser) {
        final var result = parseFunction(parent, childName, parser, true);
        if (result == null) {
            throw FatalError.parseError(parent, "Mandatory attribute \"" + childName + "\" is missing in the definition");
        }
        return result;
    }

    protected <F extends FunctionDefinition> Ref<F> parseOptionalFunction(YamlNode parent, String childName, BaseParser<F> parser) {
        if (parent.get(childName) == null) return null;
        return parseFunction(parent, childName, parser, false);
    }

    protected <S extends StreamDefinition> Ref<S> parseStream(YamlNode parent, String childName, BaseParser<S> parser) {
        return new ReferenceOrInlineDefinitionParser<>("stream", childName, parser).parse(parent);
    }

    protected StreamWrapper parseAndGetStreamWrapper(YamlNode parent) {
        TopicDefinition definition = ;
        return definition != null ? context.getStreamWrapper(definition) : null;
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
