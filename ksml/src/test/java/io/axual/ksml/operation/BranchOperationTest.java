package io.axual.ksml.operation;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
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

import io.axual.ksml.definition.BranchDefinition;
import io.axual.ksml.definition.PipelineDefinition;
import io.axual.ksml.stream.KStreamWrapper;
import org.apache.kafka.streams.kstream.BranchedKStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.axual.ksml.operation.OperationTestSupport.forEachAction;
import static io.axual.ksml.operation.OperationTestSupport.key;
import static io.axual.ksml.operation.OperationTestSupport.mockContext;
import static io.axual.ksml.operation.OperationTestSupport.operationConfig;
import static io.axual.ksml.operation.OperationTestSupport.predicate;
import static io.axual.ksml.operation.OperationTestSupport.value;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BranchOperationTest {

    @SuppressWarnings("unchecked")
    private static KStreamWrapper splittableStream(BranchedKStream<Object, Object> branched) {
        final KStream<Object, Object> stream = mock(KStream.class);
        when(stream.split(any(Named.class))).thenReturn(branched);
        return new KStreamWrapper(stream, key(), value());
    }

    @Test
    @SuppressWarnings("unchecked")
    void applyWithoutBranchesReturnsNull() {
        final BranchedKStream<Object, Object> branched = mock(BranchedKStream.class);
        when(branched.noDefaultBranch()).thenReturn(Map.of());
        final var operation = new BranchOperation(operationConfig("branch"), List.of());

        assertThat(operation.apply(splittableStream(branched), mockContext())).isNull();
    }

    @Test
    @SuppressWarnings("unchecked")
    void applyWithPredicateBranchReturnsNull() {
        final BranchedKStream<Object, Object> branched = mock(BranchedKStream.class);
        final KStream<Object, Object> branchStream = mock(KStream.class);
        when(branched.noDefaultBranch()).thenReturn(Map.of("branch0", branchStream));
        final var branch = new BranchDefinition(predicate(), new PipelineDefinition("p", null, List.of(), null));
        final var operation = new BranchOperation(operationConfig("branch"), List.of(branch));

        assertThat(operation.apply(splittableStream(branched), mockContext())).isNull();
    }

    @Test
    @SuppressWarnings("unchecked")
    void applyWithoutPredicateUsesDefaultTrueBranch() {
        final BranchedKStream<Object, Object> branched = mock(BranchedKStream.class);
        final KStream<Object, Object> branchStream = mock(KStream.class);
        when(branched.noDefaultBranch()).thenReturn(Map.of("branch0", branchStream));
        final var branch = new BranchDefinition(null, new PipelineDefinition("p", null, List.of(), null));
        final var operation = new BranchOperation(operationConfig("branch"), List.of(branch));

        assertThat(operation.apply(splittableStream(branched), mockContext())).isNull();
    }

    @Test
    @SuppressWarnings("unchecked")
    void applyRunsBranchChainOperations() {
        final BranchedKStream<Object, Object> branched = mock(BranchedKStream.class);
        final KStream<Object, Object> branchStream = mock(KStream.class);
        when(branched.noDefaultBranch()).thenReturn(Map.of("branch0", branchStream));
        final var chained = new PeekOperation(operationConfig("peek"), forEachAction());
        final var branch = new BranchDefinition(predicate(), new PipelineDefinition("p", null, List.of(chained), null));
        final var operation = new BranchOperation(operationConfig("branch"), List.of(branch));

        assertThat(operation.apply(splittableStream(branched), mockContext())).isNull();
    }

    @Test
    @SuppressWarnings("unchecked")
    void applyRunsBranchSink() {
        final BranchedKStream<Object, Object> branched = mock(BranchedKStream.class);
        final KStream<Object, Object> branchStream = mock(KStream.class);
        when(branched.noDefaultBranch()).thenReturn(Map.of("branch0", branchStream));
        final var sink = new ForEachOperation(operationConfig("forEach"), forEachAction());
        final var branch = new BranchDefinition(predicate(), new PipelineDefinition("p", null, List.of(), sink));
        final var operation = new BranchOperation(operationConfig("branch"), List.of(branch));

        assertThat(operation.apply(splittableStream(branched), mockContext())).isNull();
    }
}
