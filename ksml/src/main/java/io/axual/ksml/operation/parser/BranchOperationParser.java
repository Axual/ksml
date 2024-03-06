package io.axual.ksml.operation.parser;

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

import io.axual.ksml.definition.parser.BranchDefinitionParser;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.BranchOperation;
import io.axual.ksml.parser.StructParser;

public class BranchOperationParser extends OperationParser<BranchOperation> {
    private final boolean includePipelineSchema;

    public BranchOperationParser(TopologyResources resources, boolean includePipelineSchema) {
        super("branch", resources);
        this.includePipelineSchema = includePipelineSchema;
    }

    @Override
    public StructParser<BranchOperation> parser() {
        return structParser(
                BranchOperation.class,
                "",
                "Splits the pipeline result into multiple substreams. Each message gets sent down one stream, based on the first matching branch condition",
                operationNameField(),
                listField(
                        KSMLDSL.Operations.BRANCH,
                        "branch",
                        "Defines a single branch, consisting of a condition and a pipeline to execute for messages that fulfil the predicate",
                        new BranchDefinitionParser(resources(), includePipelineSchema)),
                (name, branches) -> branches != null ? new BranchOperation(operationConfig(namespace(), null), branches) : null);
    }
}
