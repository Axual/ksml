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
import io.axual.ksml.definition.StreamDefinition;
import io.axual.ksml.exception.KSMLParseException;
import io.axual.ksml.execution.FatalError;
import io.axual.ksml.generator.TopologyResources;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class ContextAwareParser<T> extends BaseParser<T> {
    private static final Map<String, AtomicInteger> typeInstanceCounters = new HashMap<>();
    // Name prefix for all functions and operations
    protected final String prefix;
    protected final TopologyResources resources;

    protected ContextAwareParser(String prefix, TopologyResources resources) {
        this.prefix = prefix;
        this.resources = resources;
    }

    protected <F extends FunctionDefinition> FunctionDefinition parseFunction(YamlNode parent, String childName, BaseParser<F> parser, boolean allowNull) {
        final var resource = new TopologyResourceParser<>("function", childName, resources::function, parser).parse(parent);
        if (resource == null) {
            if (allowNull) return null;
            throw new KSMLParseException(parent, "Mandatory attribute \"" + childName + "\" is missing in the definition at " + parent.toString());
        }
        return resource.definition();
    }

    protected <F extends FunctionDefinition> FunctionDefinition parseFunction(YamlNode parent, String childName, BaseParser<F> parser) {
        return parseFunction(parent, childName, parser, false);
    }

    protected <F extends FunctionDefinition> FunctionDefinition parseOptionalFunction(YamlNode parent, String childName, BaseParser<F> parser) {
        if (parent.get(childName) == null) return null;
        return parseFunction(parent, childName, parser, true);
    }

    protected StreamDefinition parseStream(YamlNode parent, String childName, BaseParser<StreamDefinition> parser) {
        final var resource = new TopologyResourceParser<>("stream", childName, resources.topics()::get, parser).parse(parent);
        if (resource != null && resource.definition() instanceof StreamDefinition def) return def;
        throw FatalError.parseError(parent, childName + " stream not defined");
    }

    protected String determineName(String name, String type) {
        if (name == null || name.trim().isEmpty()) {
            return determineName(type);
        }
        return name.trim();
    }

    protected String determineName(String type) {
        return String.format("%s_%03d", type, typeInstanceCounters.computeIfAbsent(type, t -> new AtomicInteger(1)).getAndIncrement());
    }
}
