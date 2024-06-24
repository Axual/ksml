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


import io.axual.ksml.definition.StreamDefinition;
import io.axual.ksml.definition.parser.StreamDefinitionParser;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.MergeOperation;
import io.axual.ksml.parser.StructParser;

public class MergeOperationParser extends OperationParser<MergeOperation> {
    public MergeOperationParser(TopologyResources resources) {
        super(KSMLDSL.Operations.MERGE, resources);
    }

    @Override
    protected StructParser<MergeOperation> parser() {
        return structParser(
                MergeOperation.class,
                "",
                "A merge operation to join two Streams",
                operationTypeField(),
                operationNameField(),
                topicField(KSMLDSL.Operations.Merge.STREAM, "The stream to merge with", new StreamDefinitionParser(resources(), true)),
                (type, name, stream, tags) -> {
                    if (stream instanceof StreamDefinition streamDef) {
                        return new MergeOperation(operationConfig(name, tags), streamDef);
                    }
                    throw new TopologyException("Merge stream not correct, should be a defined Stream");
                });
    }
}
