package io.axual.ksml.operation.processor;

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

import io.axual.ksml.operation.processor.TransformKeyProcessor.TransformKeyAction;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OperationProcessorSupplierTest {

    @Test
    @DisplayName("get builds a processor instance using the configured factory")
    void getCreatesProcessorViaFactory() {
        final TransformKeyAction action = (stores, rec) -> rec.key();
        final var supplier = new OperationProcessorSupplier<>(
                "mapKey", TransformKeyProcessor::new, action, new String[0]);

        assertThat(supplier.get()).isInstanceOf(TransformKeyProcessor.class);
    }
}
