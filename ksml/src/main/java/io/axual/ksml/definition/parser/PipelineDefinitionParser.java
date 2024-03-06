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


import io.axual.ksml.data.parser.NamedObjectParser;
import io.axual.ksml.definition.PipelineDefinition;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.AsOperation;
import io.axual.ksml.operation.OperationConfig;
import io.axual.ksml.operation.parser.*;
import io.axual.ksml.parser.ContextAwareParser;
import io.axual.ksml.parser.IgnoreParser;
import io.axual.ksml.parser.StructParser;

import java.util.ArrayList;

public class PipelineDefinitionParser extends ContextAwareParser<PipelineDefinition> implements NamedObjectParser {
    private final boolean parseSource;
    private String defaultName;

    protected PipelineDefinitionParser(TopologyResources resources) {
        this(resources, true);
    }

    protected PipelineDefinitionParser(TopologyResources resources, boolean parseSource) {
        super(resources);
        this.parseSource = parseSource;
    }

    @Override
    public StructParser<PipelineDefinition> parser() {
        final var asParser = new AsOperationParser(resources());
        final var branchParser = new BranchOperationParser(resources(), parseSource);
        final var forEachParser = new ForEachOperationParser(resources());
        final var printParser = new PrintOperationParser(resources());
        final var toParser = new ToOperationParser(resources());

        final var sourceField = topologyResourceField("source", KSMLDSL.Pipelines.FROM, "Pipeline source", resources()::topic, new TopicDefinitionParser(true));

        return structParser(
                PipelineDefinition.class,
                parseSource ? "" : "WithoutSource",
                "Defines a pipeline through a source, a series of operations to perform on it and a sink operation to close the stream with",
                optional(stringField(KSMLDSL.Pipelines.NAME, true, null, "The name of the pipeline. If this field is not defined, then the name is derived from the context.")),
                parseSource ? sourceField : new IgnoreParser<>(),
                optional(listField(KSMLDSL.Pipelines.VIA, "step", "A series of operations performed on the input stream", new PipelineOperationParser(resources()))),
                optional(asParser),
                optional(branchParser),
                optional(forEachParser),
                optional(printParser),
                optional(toParser),
                (name, from, via, as, branch, forEach, print, to) -> {
                    name = validateName("Pipeline", name, defaultName, false);
                    via = via != null ? via : new ArrayList<>();
                    if (as != null) return new PipelineDefinition(name, from, via, as);
                    if (branch != null) return new PipelineDefinition(name, from, via, branch);
                    if (forEach != null) return new PipelineDefinition(name, from, via, forEach);
                    if (print != null) return new PipelineDefinition(name, from, via, print);
                    if (to != null) return new PipelineDefinition(name, from, via, to);
                    // If no sink operation was specified, then we create an AS operation here with the name provided.
                    // This means that pipeline results can be referred to by other pipelines using the pipeline's name
                    // as identifier.
                    var sinkOperation = name != null ? new AsOperation(new OperationConfig(resources().getUniqueOperationName(name), null), name) : null;
                    return new PipelineDefinition(name, from, via, sinkOperation);
                });
    }

    @Override
    public void defaultName(String name) {
        this.defaultName = name;
    }
}
