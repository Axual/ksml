package io.axual.ksml.data.util;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
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

public class ValuePrinter {
    private static final String QUOTE = "\"";

    public static String print(Object value, boolean quoted) {
        var quote = quoted && value instanceof String ? QUOTE : "";
        return value != null ? quote + value + quote : "null";
    }

    private ValuePrinter() {
        // Prevent instantiation.
    }
}
