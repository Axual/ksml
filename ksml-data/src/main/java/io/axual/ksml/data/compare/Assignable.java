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
 * This class is used as a return value for isAssignableFrom methods in several classes. It can represent two states:
 * (1) OK --> indicates that an object is assignable from another object.
 * (2) Not OK --> indicates that an issue was found and the object is not assignable from the other object.
 * Since case (1) is unique (message field is null), this case is optimized by using a singleton instance.
 */
@Getter
public class Assignable {
    private static final Assignable OK = new Assignable(null, null);
    private final String message;
    private final Assignable cause;

    private Assignable(String message, Assignable cause) {
        this.message = message;
        this.cause = cause != null && cause.isNotAssignable() ? cause : null;
    }

    public static Assignable ok() {
        return OK;
    }

    public static Assignable notAssignable(String message) {
        return notAssignable(message, null);
    }

    public static Assignable notAssignable(String message, Assignable cause) {
        Objects.requireNonNull(message, "message must not be null");
        return new Assignable(message, cause);
    }

    public boolean isOK() {
        return message == null;
    }

    public boolean isNotAssignable() {
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
