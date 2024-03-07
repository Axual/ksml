package io.axual.ksml.operation;

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


import io.axual.ksml.data.object.DataBoolean;
import io.axual.ksml.definition.BranchDefinition;
import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserPredicate;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Predicate;

import java.util.List;

public class BranchOperation extends BaseOperation {
    private static final String PREDICATE_NAME = "Predicate";
    private final List<BranchDefinition> branches;

    public BranchOperation(OperationConfig config, List<BranchDefinition> branches) {
        super(config);
        this.branches = branches;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input, TopologyBuildContext context) {
        final var k = input.keyType();
        final var v = input.valueType();

        // Prepare the branch predicates to pass into the KStream
        final var predicates = new Predicate[branches.size()];
        for (var index = 0; index < branches.size(); index++) {
            final var branch = branches.get(index);
            if (branch.predicate() != null) {
                final var pred = userFunctionOf(context, PREDICATE_NAME, branch.predicate(), equalTo(DataBoolean.DATATYPE), superOf(k), superOf(v));
                predicates[index] = new UserPredicate(pred);
            } else {
                predicates[index] = (key, value) -> true;
            }
        }

        // Pass the predicates to KStream and get resulting KStream branches back
        @SuppressWarnings("unchecked") final KStream<Object, Object>[] output = name != null
                ? input.stream.branch(Named.as(name), predicates)
                : input.stream.branch(predicates);

        // For every branch, generate a separate pipeline
        for (var index = 0; index < output.length; index++) {
            StreamWrapper branchCursor = new KStreamWrapper(output[index], k, v);
            for (StreamOperation operation : branches.get(index).pipeline().chain()) {
                branchCursor = branchCursor.apply(operation, context);
            }
            if (branches.get(index).pipeline().sink() != null) {
                branchCursor.apply(branches.get(index).pipeline().sink(), context);
            }
        }

        return null;
    }
}
