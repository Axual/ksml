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
import io.axual.ksml.execution.FatalError;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.MergeOperation;
import io.axual.ksml.operation.OperationConfig;
import io.axual.ksml.parser.StructParser;

public class MergeOperationParser extends OperationParser<MergeOperation> {
    public MergeOperationParser(TopologyResources resources) {
        super("merge", resources);
    }

    @Override
    protected StructParser<MergeOperation> parser() {
        return structParser(
                MergeOperation.class,
                "A merge operation to join two Streams",
                stringField(KSMLDSL.Operations.TYPE_ATTRIBUTE, true, "The type of the operation, fixed value \"" + KSMLDSL.Operations.MERGE + "\""),
                nameField(),
                topicField(KSMLDSL.Operations.Merge.STREAM, true, "The stream to merge with", new StreamDefinitionParser()),
                (type, name, stream) -> {
                    if (stream instanceof StreamDefinition streamDef) {
                        return new MergeOperation(new OperationConfig(namespace(), name, null), streamDef);
                    }
                    throw FatalError.topologyError("Merge stream not correct, should be a defined Stream");
                });
    }
}
