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

import org.junit.jupiter.api.Test;

import static io.axual.ksml.operation.OperationTestSupport.cogroupedStream;
import static io.axual.ksml.operation.OperationTestSupport.globalKTable;
import static io.axual.ksml.operation.OperationTestSupport.groupedStream;
import static io.axual.ksml.operation.OperationTestSupport.groupedTable;
import static io.axual.ksml.operation.OperationTestSupport.kStream;
import static io.axual.ksml.operation.OperationTestSupport.kTable;
import static io.axual.ksml.operation.OperationTestSupport.mockContext;
import static io.axual.ksml.operation.OperationTestSupport.operationConfig;
import static io.axual.ksml.operation.OperationTestSupport.sessionWindowed;
import static io.axual.ksml.operation.OperationTestSupport.sessionWindowedCogrouped;
import static io.axual.ksml.operation.OperationTestSupport.timeWindowed;
import static io.axual.ksml.operation.OperationTestSupport.timeWindowedCogrouped;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

class AsOperationTest {

    private AsOperation operation() {
        return new AsOperation(operationConfig("as"), "myTarget");
    }

    @Test
    void applyToStreamRegistersUnderTargetNameAndReturnsNull() {
        final var context = mockContext();
        final var input = kStream();

        assertThat(operation().apply(input, context)).isNull();
        verify(context).registerStreamWrapper(eq("myTarget"), eq(input));
    }

    @Test
    void applyToTableReturnsNull() {
        assertThat(operation().apply(kTable(), mockContext())).isNull();
    }

    @Test
    void applyToGlobalTableReturnsNull() {
        assertThat(operation().apply(globalKTable(), mockContext())).isNull();
    }

    @Test
    void applyToGroupedStreamReturnsNull() {
        assertThat(operation().apply(groupedStream(), mockContext())).isNull();
    }

    @Test
    void applyToGroupedTableReturnsNull() {
        assertThat(operation().apply(groupedTable(), mockContext())).isNull();
    }

    @Test
    void applyToSessionWindowedReturnsNull() {
        assertThat(operation().apply(sessionWindowed(), mockContext())).isNull();
    }

    @Test
    void applyToTimeWindowedReturnsNull() {
        assertThat(operation().apply(timeWindowed(), mockContext())).isNull();
    }

    @Test
    void applyToCogroupedStreamReturnsNull() {
        assertThat(operation().apply(cogroupedStream(), mockContext())).isNull();
    }

    @Test
    void applyToSessionWindowedCogroupedReturnsNull() {
        assertThat(operation().apply(sessionWindowedCogrouped(), mockContext())).isNull();
    }

    @Test
    void applyToTimeWindowedCogroupedReturnsNull() {
        assertThat(operation().apply(timeWindowedCogrouped(), mockContext())).isNull();
    }
}
