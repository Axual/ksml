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
import io.axual.ksml.exception.ParseException;
import io.axual.ksml.parser.FieldParsers;
import io.axual.ksml.definition.BranchDefinition;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.parser.DefinitionParser;
import io.axual.ksml.parser.NamedObjectParser;
import io.axual.ksml.parser.ParseNode;
import io.axual.ksml.parser.StructsParser;
import io.axual.ksml.parser.TopologyResourceFields;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public class BranchDefinitionParser extends DefinitionParser<BranchDefinition> implements NamedObjectParser {
    private final TopologyResourceFields resourceFields;
    private final boolean includePipelineSchema;
    private final PipelineDefinitionParser pipelineParser;

    public BranchDefinitionParser(TopologyResources resources, boolean includePipelineSchema) {
        this.resourceFields = new TopologyResourceFields(resources);
        this.includePipelineSchema = includePipelineSchema;
        pipelineParser = new PipelineDefinitionParser(resourceFields.resources(), false);
    }

    @Override
    public StructsParser<BranchDefinition> parser() {
        // This parser uses the PipelineDefinitionParser recursively, hence requires a special implementation to not
        // make the associated DataSchema recurse infinitely.
        final var predParser = FieldParsers.optional(resourceFields.functionField(KSMLDSL.Operations.Branch.PREDICATE, "Defines the condition under which messages get sent down this branch", new PredicateDefinitionParser(false)));
        if (includePipelineSchema) return FieldParsers.structsParser(
                BranchDefinition.class,
                "WithPipeline",
                "Defines a branch with sub-pipeline in a BranchOperation",
                predParser,
                pipelineParser,
                (predicate, pipeline, tags) -> new BranchDefinition(predicate, pipeline)
        );
        return nestedBranchNotSupported(FieldParsers.structsParser(
                BranchDefinition.class,
                "",
                "Defines a branch without sub-pipeline in a BranchOperation",
                predParser,
                (predicate, tags) -> new BranchDefinition(predicate, null)
        ));
    }

    /**
     * The parser without sub-pipeline is only used for a branch inside a branch, which is not supported
     * yet. Report that clearly, instead of reporting its fields as unknown.
     */
    private static StructsParser<BranchDefinition> nestedBranchNotSupported(StructsParser<BranchDefinition> parser) {
        return new StructsParser<>() {
            @Override
            public BranchDefinition parse(ParseNode node) {
                throw new ParseException(node, "A branch inside a branch is not supported yet. End the outer branch with \"as\" instead, and branch again in a second pipeline");
            }

            @Override
            public List<StructSchema> schemas() {
                return parser.schemas();
            }

            // Skip the unknown-field check, so the error above is reported
            @Override
            public Optional<Set<String>> knownFields(ParseNode node) {
                return Optional.empty();
            }
        };
    }

    @Override
    public void defaultShortName(String name) {
        pipelineParser.defaultShortName(name);
    }

    @Override
    public void defaultLongName(String name) {
        pipelineParser.defaultLongName(name);
    }
}
