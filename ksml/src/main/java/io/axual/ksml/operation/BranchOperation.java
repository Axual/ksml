package io.axual.ksml.operation;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 Axual B.V.
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


import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Predicate;

import java.util.List;

import io.axual.ksml.definition.BranchDefinition;
import io.axual.ksml.parser.StreamOperation;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserPredicate;

public class BranchOperation extends BaseOperation {
    private final List<BranchDefinition> branches;

    public BranchOperation(String name, List<BranchDefinition> branches) {
        super(name);
        this.branches = branches;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input) {
        // Prepare the branch predicates to pass into the KStream
        @SuppressWarnings("unchecked")
        var predicates = new Predicate[branches.size()];
        for (var index = 0; index < branches.size(); index++) {
            predicates[index] = getBranchPredicate(branches.get(index));
        }

        // Pass the predicates to KStream and get resulting KStream branches back
        @SuppressWarnings("unchecked")
        KStream<Object, Object>[] resultStreams = input.stream.branch(Named.as(name), predicates);

        // For every branch, generate a separate pipeline
        for (var index = 0; index < resultStreams.length; index++) {
            StreamWrapper branchCursor = new KStreamWrapper(resultStreams[index], input.keyType, input.valueType);
            for (StreamOperation operation : branches.get(index).pipeline.chain) {
                branchCursor = branchCursor.apply(operation);
            }
            if (branches.get(index).pipeline.sink != null) {
                branchCursor.apply(branches.get(index).pipeline.sink);
            }
        }

        return null;
    }

    private static Predicate<Object, Object> getBranchPredicate(BranchDefinition definition) {
        if (definition.predicate != null) {
            return new UserPredicate(definition.predicate);
        }
        return (k, v) -> true;
    }
}
