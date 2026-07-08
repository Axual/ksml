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
import io.axual.ksml.stream.CogroupedKStreamWrapper;
import org.junit.jupiter.api.Test;

import static io.axual.ksml.operation.OperationTestSupport.aggregator;
import static io.axual.ksml.operation.OperationTestSupport.cogroupedStream;
import static io.axual.ksml.operation.OperationTestSupport.groupedStream;
import static io.axual.ksml.operation.OperationTestSupport.mockContext;
import static io.axual.ksml.operation.OperationTestSupport.operationConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CogroupOperationTest {

    private CogroupOperation operation() {
        return new CogroupOperation(operationConfig("cogroup"), aggregator());
    }

    @Test
    void applyToGroupedStreamReturnsCogrouped() {
        assertThat(operation().apply(groupedStream(), mockContext())).isInstanceOf(CogroupedKStreamWrapper.class);
    }

    @Test
    void applyToCogroupedStreamIsUnsupported() {
        final var operation = operation();
        final var input = cogroupedStream();
        final var context = mockContext();
        assertThatThrownBy(() -> operation.apply(input, context))
                .isInstanceOf(TopologyException.class)
                .hasMessageContaining("not supported");
    }
}
