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

import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.stream.StreamWrapper;
import org.junit.jupiter.api.Test;

import java.util.function.Function;

import static io.axual.ksml.operation.OperationTestSupport.cogroupedStream;
import static io.axual.ksml.operation.OperationTestSupport.globalKTable;
import static io.axual.ksml.operation.OperationTestSupport.groupedStream;
import static io.axual.ksml.operation.OperationTestSupport.groupedTable;
import static io.axual.ksml.operation.OperationTestSupport.kTable;
import static io.axual.ksml.operation.OperationTestSupport.mockContext;
import static io.axual.ksml.operation.OperationTestSupport.operationConfig;
import static io.axual.ksml.operation.OperationTestSupport.sessionWindowed;
import static io.axual.ksml.operation.OperationTestSupport.sessionWindowedCogrouped;
import static io.axual.ksml.operation.OperationTestSupport.streamDefinition;
import static io.axual.ksml.operation.OperationTestSupport.timeWindowed;
import static io.axual.ksml.operation.OperationTestSupport.timeWindowedCogrouped;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies the default {@link StreamOperation} apply methods, which reject input types an operation
 * does not support. {@link MergeOperation} only overrides the {@code KStreamWrapper} variant, so
 * every other input type falls through to the throwing default.
 */
class StreamOperationTest {

    private final MergeOperation operation = new MergeOperation(operationConfig("merge"), streamDefinition());

    private void assertRejects(Function<StreamOperation, StreamWrapper> application) {
        assertThatThrownBy(() -> application.apply(operation))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("Can not apply");
    }

    @Test
    void rejectsUnsupportedInputTypes() {
        final var context = mockContext();
        assertRejects(op -> op.apply(kTable(), context));
        assertRejects(op -> op.apply(globalKTable(), context));
        assertRejects(op -> op.apply(groupedStream(), context));
        assertRejects(op -> op.apply(groupedTable(), context));
        assertRejects(op -> op.apply(sessionWindowed(), context));
        assertRejects(op -> op.apply(timeWindowed(), context));
        assertRejects(op -> op.apply(cogroupedStream(), context));
        assertRejects(op -> op.apply(sessionWindowedCogrouped(), context));
        assertRejects(op -> op.apply(timeWindowedCogrouped(), context));
    }
}
