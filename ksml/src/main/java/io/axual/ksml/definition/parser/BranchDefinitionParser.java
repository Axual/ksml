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

import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.definition.BranchDefinition;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.parser.ContextAwareParser;
import io.axual.ksml.parser.StructParser;
import io.axual.ksml.util.ListUtil;

public class BranchDefinitionParser extends ContextAwareParser<BranchDefinition> {
    private final boolean includePipelineSchema;

    public BranchDefinitionParser(TopologyResources resources, boolean includePipelineSchema) {
        super(resources);
        this.includePipelineSchema = includePipelineSchema;
    }

    @Override
    public StructParser<BranchDefinition> parser() {
        // This parser uses the PipelineDefinitionParser recursively, hence requires a special implementation to not
        // make the associated DataSchema recurse infinitely.
        final var predParser = optional(functionField(KSMLDSL.Operations.Branch.PREDICATE, "Defines the condition under which messages get sent down this branch", new PredicateDefinitionParser()));
        final var pipelineParser = new PipelineDefinitionParser(resources(), false);
        final StructSchema schema = includePipelineSchema
                ? structSchema(BranchDefinition.class.getSimpleName(), "Defines one branch in a BranchOperation", ListUtil.union(predParser.fields(), pipelineParser.fields()))
                : structSchema(BranchDefinition.class.getSimpleName(), "Defines one branch in a BranchOperation", predParser.fields());
        return StructParser.of(node -> new BranchDefinition(predParser.parse(node), pipelineParser.parse(node)), schema);
    }
}
