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
import io.axual.ksml.operation.AsOperation;
import io.axual.ksml.operation.OperationConfig;
import io.axual.ksml.operation.parser.AsOperationParser;
import io.axual.ksml.operation.parser.BranchOperationParser;
import io.axual.ksml.operation.parser.ForEachOperationParser;
import io.axual.ksml.operation.parser.PipelineOperationParser;
import io.axual.ksml.operation.parser.PrintOperationParser;
import io.axual.ksml.operation.parser.ToOperationParser;
import io.axual.ksml.operation.parser.ToTopicNameExtractorOperationParser;
import io.axual.ksml.parser.IgnoreParser;
import io.axual.ksml.parser.NamedObjectParser;
import io.axual.ksml.parser.StructsParser;
import io.axual.ksml.parser.TopologyResourceAwareParser;

import java.util.ArrayList;

public class PipelineDefinitionParser extends TopologyResourceAwareParser<PipelineDefinition> implements NamedObjectParser {
    private final boolean parseSource;
    private final AsOperationParser asParser;
    private final BranchOperationParser branchParser;
    private final ForEachOperationParser forEachParser;
    private final PrintOperationParser printParser;
    private final ToOperationParser toTopic;
    private final ToTopicNameExtractorOperationParser toTne;
    private String defaultShortName;
    private String defaultLongName;

    protected PipelineDefinitionParser(TopologyResources resources) {
        this(resources, true);
    }

    protected PipelineDefinitionParser(TopologyResources resources, boolean parseSource) {
        super(resources);
        this.parseSource = parseSource;
        asParser = new AsOperationParser(resources());
        branchParser = new BranchOperationParser(resources(), parseSource);
        forEachParser = new ForEachOperationParser(resources());
        printParser = new PrintOperationParser(resources());
        toTopic = new ToOperationParser(resources());
        toTne = new ToTopicNameExtractorOperationParser(resources());
    }

    @Override
    public StructsParser<PipelineDefinition> parser() {
        final var sourceField = topologyResourceField("source", KSMLDSL.Pipelines.FROM, "Pipeline source", (name, tags) -> resources().topic(name), new TopicDefinitionParser(resources(), true));

        return structsParser(
                PipelineDefinition.class,
                parseSource ? "" : "WithoutSource",
                "Defines a pipeline through a source, a series of operations to perform on it and a sink operation to close the stream with",
                optional(stringField(KSMLDSL.Pipelines.NAME, true, "The name of the pipeline. If this field is not defined, then the name is derived from the context.")),
                parseSource ? sourceField : new IgnoreParser<>(),
                optional(listField(KSMLDSL.Pipelines.VIA, "step", "step", "A series of operations performed on the input stream", new PipelineOperationParser(resources()))),
                optional(asParser),
                optional(branchParser),
                optional(forEachParser),
                optional(printParser),
                optional(toTopic),
                optional(toTne),
                (name, from, via, as, branch, forEach, print, toTopic, toTne, tags) -> {
                    final var shortName = validateName("Pipeline", name, defaultShortName, true);
                    final var longName = validateName("Pipeline", name, defaultLongName, false);
                    via = via != null ? via : new ArrayList<>();
                    if (as != null) return new PipelineDefinition(longName, from, via, as);
                    if (branch != null) return new PipelineDefinition(longName, from, via, branch);
                    if (forEach != null) return new PipelineDefinition(longName, from, via, forEach);
                    if (print != null) return new PipelineDefinition(longName, from, via, print);
                    if (toTopic != null) return new PipelineDefinition(longName, from, via, toTopic);
                    if (toTne != null) return new PipelineDefinition(longName, from, via, toTne);
                    // If no sink operation was specified, then we create an AS operation here with the name provided.
                    // This means that pipeline results can be referred to by other pipelines using the pipeline's name
                    // as identifier.
                    var sinkOperation = shortName != null ? new AsOperation(new OperationConfig(resources().getUniqueOperationName(longName), tags), shortName) : null;
                    return new PipelineDefinition(name, from, via, sinkOperation);
                });
    }

    @Override
    public void defaultShortName(String name) {
        this.defaultShortName = name;
        asParser.defaultShortName(name);
        branchParser.defaultShortName(name);
        forEachParser.defaultShortName(name);
        printParser.defaultShortName(name);
        toTopic.defaultShortName(name);
        toTne.defaultShortName(name);
    }

    @Override
    public void defaultLongName(String name) {
        this.defaultLongName = name;
        asParser.defaultLongName(name);
        branchParser.defaultLongName(name);
        forEachParser.defaultLongName(name);
        printParser.defaultLongName(name);
        toTopic.defaultLongName(name);
        toTne.defaultLongName(name);
    }
}
