package io.axual.ksml.operation;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2026 Axual B.V.
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

import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

/** Applies the operations of a pipeline in order, and connects punctuate operations to named operations after them. */
public final class OperationChain {
    // Operations after one of these run behind a repartition topic, in another sub-topology. A side path to them
    // would skip the repartition, so only the operation itself can still be a target.
    private static final List<Class<? extends StreamOperation>> REPARTITIONING_OPERATIONS = List.of(
            RepartitionOperation.class, GroupByOperation.class, GroupByKeyOperation.class, ToTableOperation.class,
            JoinWithStreamOperation.class, LeftJoinWithStreamOperation.class, OuterJoinWithStreamOperation.class,
            JoinWithTableOperation.class, LeftJoinWithTableOperation.class, OuterJoinWithTableOperation.class);

    private OperationChain() {
    }

    /**
     * Applies the operations to the cursor.
     *
     * @param cursor     the stream to start with
     * @param operations the operations of the pipeline, in order
     * @param context    the build context
     * @return the result of the last operation
     */
    public static StreamWrapper apply(StreamWrapper cursor, List<StreamOperation> operations, TopologyBuildContext context) {
        return apply(cursor, operations, context, (operation, result) -> {
        });
    }

    /**
     * Applies the operations to the cursor.
     *
     * @param cursor     the stream to start with
     * @param operations the operations of the pipeline, in order
     * @param context    the build context
     * @param applied    called with each operation and its result
     * @return the result of the last operation
     */
    public static StreamWrapper apply(StreamWrapper cursor, List<StreamOperation> operations, TopologyBuildContext context, BiConsumer<StreamOperation, StreamWrapper> applied) {
        final var punctuates = new ArrayList<PunctuateOperation>();
        for (var index = 0; index < operations.size(); index++) {
            final var operation = operations.get(index);
            // A named operation with a stream input after a punctuate operation gets a side path from it
            if (!punctuates.isEmpty() && cursor instanceof KStreamWrapper stream && isNamed(operation)) {
                for (final var punctuate : punctuates) stream = punctuate.sidePathTo((BaseOperation) operation, stream);
                cursor = stream;
            }
            if (operation instanceof PunctuateOperation punctuate)
                punctuate.namedOperationsFollow(namedTargetFollows(operations.subList(index + 1, operations.size())));
            cursor = cursor.apply(operation, context);
            if (operation instanceof PunctuateOperation punctuate) punctuates.add(punctuate);
            if (isRepartitioningOperation(operation)) punctuates.clear();
            applied.accept(operation, cursor);
        }
        return cursor;
    }

    // Whether a named operation follows, up to and including the first operation that repartitions
    private static boolean namedTargetFollows(List<StreamOperation> operations) {
        for (final var operation : operations) {
            if (isNamed(operation)) return true;
            if (isRepartitioningOperation(operation)) return false;
        }
        return false;
    }

    private static boolean isRepartitioningOperation(StreamOperation operation) {
        return REPARTITIONING_OPERATIONS.stream().anyMatch(type -> type.isInstance(operation));
    }

    private static boolean isNamed(StreamOperation operation) {
        return operation instanceof BaseOperation baseOperation && baseOperation.definedName() != null;
    }
}
