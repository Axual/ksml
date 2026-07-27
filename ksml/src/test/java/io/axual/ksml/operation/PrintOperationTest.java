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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.axual.ksml.operation.OperationTestSupport.keyValuePrinter;
import static io.axual.ksml.operation.OperationTestSupport.kStream;
import static io.axual.ksml.operation.OperationTestSupport.mockContext;
import static io.axual.ksml.operation.OperationTestSupport.operationConfig;
import static org.assertj.core.api.Assertions.assertThat;

class PrintOperationTest extends OperationTestBase {

    @Test
    @DisplayName("print with a key-value mapper is a terminal operation and returns null")
    void applyWithMapperIsTerminalAndReturnsNull() {
        final var operation = new PrintOperation(operationConfig("print"), null, "label", keyValuePrinter());
        assertThat(operation.apply(kStream(), mockContext())).isNull();
    }

    @Test
    @DisplayName("print without a mapper is a terminal operation and returns null")
    void applyWithoutMapperIsTerminalAndReturnsNull() {
        final var operation = new PrintOperation(operationConfig("print"), null, null, null);
        assertThat(operation.apply(kStream(), mockContext())).isNull();
    }
}
