package io.axual.ksml.data.exception;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
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

import io.axual.ksml.data.compare.Assignable;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataLong;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DataExceptionTest {

    @Test
    @DisplayName("Message and cause constructors retain their inputs")
    void messageAndCause() {
        assertThat(new DataException("boom")).hasMessageContaining("boom");

        final var cause = new IllegalStateException("root");
        final var exception = new DataException("boom", cause);
        assertThat(exception).hasMessageContaining("boom");
        assertThat(exception.getCause()).isSameAs(cause);
    }

    @Test
    @DisplayName("conversionFailed factories describe the source and target types")
    void conversionFailedFactories() {
        assertThat(DataException.conversionFailed(DataInteger.DATATYPE, DataLong.DATATYPE))
                .message().isNotBlank();
        assertThat(DataException.conversionFailed(DataInteger.DATATYPE, DataLong.DATATYPE, Assignable.notAssignable("reason")))
                .hasMessageContaining("reason");
        assertThat(DataException.conversionFailed("fromType", "toType"))
                .hasMessageContaining("fromType").hasMessageContaining("toType");
    }
}
