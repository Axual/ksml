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

import io.axual.ksml.data.object.DataString;

/**
 * Small utility to render objects as strings with optional quoting for Strings.
 *
 * <p>Used by diagnostic and equality-reporting code to produce consistent, human-readable
 * representations of values while preserving null vs. non-null distinctions.</p>
 */
public abstract class ValuePrinter {
    public record ValuePrinterDict(String quoteStr, String nullStr, String trueStr, String falseStr) {
    }

    private final ValuePrinterDict dict;

    protected ValuePrinter(ValuePrinterDict dict) {
        this.dict = dict;
    }

    public String print(Object value, boolean quoted) {
        final var quote = quoted && (value instanceof String || value instanceof DataString) ? dict.quoteStr() : "";
        return switch (value) {
            case null -> dict.nullStr();
            case Boolean val -> val ? dict.trueStr() : dict.falseStr();
            default -> quote + value + quote;
        };
    }
}
