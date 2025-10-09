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

import java.util.Objects;

/**
 * This class is used as a return value for the {@link DataEquals::equals} interface method. It can represent two states:
 * (1) OK --> indicates that an object is equal to another object.
 * (2) Not OK --> indicates that an issue was found and the two objects are not equal.
 * Since case (1) is unique (message field is null), this case is optimized by using a singleton instance.
 */
@Getter
public class Equal {
    private static final Equal OK = new Equal(null, null);
    private final String message;
    private final Equal cause;

    private Equal(String message, Equal cause) {
        this.message = message;
        this.cause = cause != null && cause.isNotEqual() ? cause : null;
    }

    public static Equal ok() {
        return OK;
    }

    public static Equal notEqual(String message) {
        return notEqual(message, null);
    }

    public static Equal notEqual(String message, Equal cause) {
        Objects.requireNonNull(message, "errorMessage must not be null");
        return new Equal(message, cause);
    }

    public boolean isEqual() {
        return message == null;
    }

    public boolean isNotEqual() {
        return message != null;
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
            builder.append(i.message);
        }
        return builder.toString();
    }
}
