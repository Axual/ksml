package io.axual.ksml.user;

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

import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.type.UserType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.axual.ksml.user.UserTestSupport.functionReturning;
import static io.axual.ksml.user.UserTestSupport.tags;
import static org.assertj.core.api.Assertions.assertThat;

class UserKeyValuePrinterTest {

    private static final UserType STRING = new UserType(DataString.DATATYPE);

    @Test
    @DisplayName("a string result is returned as-is by the printer")
    void returnsStringResultDirectly() {
        final var printer = new UserKeyValuePrinter(functionReturning(STRING, 2, new DataString("printed")), tags());
        assertThat(printer.apply("key", "value")).isEqualTo("printed");
    }

    @Test
    @DisplayName("a non-string result falls back to its toString representation")
    void fallsBackToToStringForNonStringResult() {
        final var printer = new UserKeyValuePrinter(functionReturning(STRING, 2, new DataInteger(7)), tags());
        assertThat(printer.apply("key", "value")).isEqualTo("7");
    }
}
