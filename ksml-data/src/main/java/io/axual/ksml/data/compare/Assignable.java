package io.axual.ksml.data.compare;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
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
public class Assignable {
    private static final Assignable OK = new Assignable(null, null);
    private final String errorMessage;
    private final Assignable cause;

    private Assignable(String errorMessage, Assignable cause) {
        this.errorMessage = errorMessage;
        this.cause = cause != null && cause.isError() ? cause : null;
    }

    public static Assignable ok() {
        return OK;
    }

    public static Assignable error(String errorMessage) {
        return error(errorMessage, null);
    }

    public static Assignable error(String errorMessage, Assignable cause) {
        return errorMessage != null ? new Assignable(errorMessage, cause) : ok();
    }

    public boolean isOK() {
        return errorMessage == null;
    }

    public boolean isError() {
        return errorMessage != null;
    }

    @Override
    public String toString() {
        return toString(false);
    }

    public String toString(boolean prefixFirstLine) {
        return toString("caused by: ", prefixFirstLine);
    }

    public String toString(String linePrefix, boolean prefixFirstLine) {
        final var builder = new StringBuilder();
        for (var i = this; i != null; i = i.cause) {
            if (i != this) builder.append("\n");
            if (i != this || prefixFirstLine) builder.append(linePrefix);
            builder.append(i.errorMessage);
        }
        return builder.toString();
    }
}
