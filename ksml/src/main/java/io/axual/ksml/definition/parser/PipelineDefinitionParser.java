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


import io.axual.ksml.definition.PipelineDefinition;
import io.axual.ksml.operation.parser.PipelineOperationParser;
import io.axual.ksml.operation.parser.PipelineSinkOperationParser;
import io.axual.ksml.parser.ContextAwareParser;
import io.axual.ksml.parser.ListParser;
import io.axual.ksml.parser.ParseContext;
import io.axual.ksml.parser.ReferenceOrInlineParser;
import io.axual.ksml.parser.YamlNode;

import static io.axual.ksml.dsl.KSMLDSL.PIPELINE_FROM_ATTRIBUTE;
import static io.axual.ksml.dsl.KSMLDSL.PIPELINE_VIA_ATTRIBUTE;

public class PipelineDefinitionParser extends ContextAwareParser<PipelineDefinition> {
    public PipelineDefinitionParser(ParseContext context) {
        super(context);
    }

    @Override
    public PipelineDefinition parse(YamlNode node) {
        return parse(node, true, true);
    }

    public PipelineDefinition parse(YamlNode node, boolean parseSource, boolean parseSink) {
        if (node == null) return null;
        return new PipelineDefinition(
                parseSource
                        ? new ReferenceOrInlineParser<>("source", PIPELINE_FROM_ATTRIBUTE, context.getStreamDefinitions()::get, new StreamDefinitionParser()).parse(node)
                        : null,
                new ListParser<>(new PipelineOperationParser(context)).parse(node.get(PIPELINE_VIA_ATTRIBUTE, "step")),
                parseSink ? new PipelineSinkOperationParser(context).parse(node) : null);
    }
}
