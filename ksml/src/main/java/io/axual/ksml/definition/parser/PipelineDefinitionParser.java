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


import io.axual.ksml.definition.PipelineDefinition;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.parser.AsOperationParser;
import io.axual.ksml.operation.parser.BranchOperationParser;
import io.axual.ksml.operation.parser.ForEachOperationParser;
import io.axual.ksml.operation.parser.PipelineOperationParser;
import io.axual.ksml.operation.parser.PrintOperationParser;
import io.axual.ksml.operation.parser.ToOperationParser;
import io.axual.ksml.parser.ContextAwareParser;
import io.axual.ksml.parser.StructParser;

import java.util.ArrayList;

public class PipelineDefinitionParser extends ContextAwareParser<PipelineDefinition> {
    private final boolean parseSource;

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

        return structParser(
                PipelineDefinition.class,
                "Defines a pipeline through a source, a series of operations to perform on it and a sink operation to close the stream with",
                topologyResourceField("source", KSMLDSL.Pipelines.FROM, "Pipeline source", resources()::topic, new TopicDefinitionParser()),
                listField(KSMLDSL.Pipelines.VIA, "step", false, "A series of operations performend on the input stream", new PipelineOperationParser(resources())),
                optional(asParser),
                branchParser,
                forEachParser,
                printParser,
                toParser,
                (from, via, as, branch, forEach, print, to) -> {
                    via = via != null ? via : new ArrayList<>();
                    if (as != null) return new PipelineDefinition(from, via, as);
                    if (branch != null) return new PipelineDefinition(from, via, branch);
                    if (forEach != null) return new PipelineDefinition(from, via, forEach);
                    if (print != null) return new PipelineDefinition(from, via, print);
                    if (to != null) return new PipelineDefinition(from, via, to);
                    return new PipelineDefinition(from, via, null);
                });
    }
}
