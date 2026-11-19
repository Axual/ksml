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
import io.axual.ksml.parser.FieldParsers;
import io.axual.ksml.definition.BranchDefinition;
import io.axual.ksml.definition.FunctionDefinition;
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
        final var predParser = FieldParsers.optional(resourceFields.functionField(KSMLDSL.Operations.Branch.PREDICATE, "Defines the condition under which messages get sent down this branch", new PredicateDefinitionParser(false)));
        if (includePipelineSchema) return withPipelineParser(predParser);
        return nestedBranchParser(predParser);
    }

    private StructsParser<BranchDefinition> withPipelineParser(StructsParser<FunctionDefinition> predParser) {
        return FieldParsers.structsParser(
                BranchDefinition.class,
                "WithPipeline",
                "Defines a branch with sub-pipeline in a BranchOperation",
                predParser,
                pipelineParser,
                (predicate, pipeline, tags) -> new BranchDefinition(predicate, pipeline)
        );
    }

    /**
     * The parser for a branch inside a branch. It parses the branch with its sub-pipeline, like the outer
     * level does. Only its schema is shorter: it lists the condition and allows any other field. A full
     * schema would contain the pipeline, with a branch, with a pipeline, and so on forever.
     * <p>
     * The full parser is only made when a nested branch is parsed. Making it needs the schema of the
     * sub-pipeline, which contains the next nested level, so making it eagerly would never end.
     */
    private StructsParser<BranchDefinition> nestedBranchParser(StructsParser<FunctionDefinition> predicateParser) {
        final var schemas = FieldParsers.structsParser(
                        BranchDefinition.class,
                        "",
                        "Defines a branch inside a branch. Its sub-pipeline has the same fields as a pipeline, without \"from\"",
                        predicateParser,
                        (predicate, _) -> new BranchDefinition(predicate, null))
                .schemas().stream()
                .map(schema -> new StructSchema(schema.namespace(), schema.name(), schema.doc(), schema.fields(), true))
                .toList();
        final var fullParser = StructsParser.lazy(() -> withPipelineParser(predicateParser));
        return new StructsParser<>() {
            @Override
            public BranchDefinition parse(ParseNode node) {
                return fullParser.parse(node);
            }

            @Override
            public List<StructSchema> schemas() {
                return schemas;
            }

            // The schema allows any field, but the parser checks the fields of a full branch
            @Override
            public Optional<Set<String>> knownFields(ParseNode node) {
                return fullParser.knownFields(node);
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
