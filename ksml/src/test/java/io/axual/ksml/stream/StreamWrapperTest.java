package io.axual.ksml.stream;

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

import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.operation.StreamOperation;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

class StreamWrapperTest {

    /** Minimal wrapper relying on the default {@link StreamWrapper#apply} implementation. */
    private static final class BareStreamWrapper implements StreamWrapper {
        @Override
        public StreamDataType keyType() {
            return null;
        }

        @Override
        public StreamDataType valueType() {
            return null;
        }
    }

    @Test
    @DisplayName("the default apply implementation rejects an unsupported operation")
    void defaultApplyRejectsUnsupportedOperation() {
        final var wrapper = new BareStreamWrapper();
        final var operation = mock(StreamOperation.class);

        assertThatThrownBy(() -> wrapper.apply(operation, null))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Can not apply");
    }
}
