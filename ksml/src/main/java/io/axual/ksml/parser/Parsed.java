package io.axual.ksml.parser;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
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

import lombok.Getter;

@Getter
public class Parsed<T> {
    private final T result;
    private final String errorMessage;

    private Parsed(T result, String errorMessage) {
        this.result = result;
        this.errorMessage = errorMessage;
    }

    public static <T> Parsed<T> ok(T result) {
        return new Parsed<>(result, null);
    }

    public static <T> Parsed<T> error(String errorMessage) {
        return new Parsed<>(null, errorMessage);
    }

    public boolean isOk() {
        return errorMessage == null;
    }

    public boolean isError() {
        return errorMessage != null;
    }
}
