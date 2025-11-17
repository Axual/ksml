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
import lombok.NonNull;

/**
 * Represents the result of an assignability check (typically used by isAssignableFrom-like methods).
 * <p>
 * The result can be in two states:
 * <ul>
 *   <li>OK — the source type/value can be assigned to the target type/value.</li>
 *   <li>Not OK — the assignment is invalid, accompanied by a human-readable explanation, optionally with a cause chain.</li>
 * </ul>
 * Failures can be chained using a cause, enabling hierarchical explanations of why assignability does not hold.
 * To optimize for the common success case, the OK state is implemented as a singleton instance (message is {@code null}).
 */
@Getter
public class Assignable {
    private static final Assignable ASSIGNABLE = new Assignable(null, null);
    private final String message;
    private final Assignable cause;

    /**
     * Creates a new Assignable result.
     *
     * @param message a human-readable explanation of the failed assignment; {@code null} means assignment is allowed
     * @param cause   an optional underlying reason; only stored when it represents a non-assignable state
     */
    private Assignable(String message, Assignable cause) {
        this.message = message;
        this.cause = cause != null && cause.isNotAssignable() ? cause : null;
    }

    /**
     * Returns the singleton instance representing a successful assignability check.
     *
     * @return the OK Assignable instance
     */
    public static Assignable assignable() {
        return ASSIGNABLE;
    }

    /**
     * Creates a not-assignable result with a human-readable message.
     *
     * @param message explanation of why the assignment is invalid
     * @return a new Assignable instance representing a failure
     */
    public static Assignable notAssignable(String message) {
        return notAssignable(message, null);
    }

    /**
     * Creates a not-assignable result with a message and an optional underlying cause.
     *
     * @param message explanation of why the assignment is invalid
     * @param cause   an optional underlying reason providing more detail
     * @return a new Assignable instance representing a failure
     * @throws NullPointerException if {@code message} is {@code null}
     */
    public static Assignable notAssignable(@NonNull String message, Assignable cause) {
        return new Assignable(message, cause);
    }

    /**
     * Indicates whether assignment is allowed.
     *
     * @return {@code true} if assignment is allowed; {@code false} otherwise
     */
    public boolean isAssignable() {
        return message == null;
    }

    /**
     * Indicates whether assignment is not allowed.
     *
     * @return {@code true} if assignment is not allowed; {@code false} otherwise
     */
    public boolean isNotAssignable() {
        return message != null;
    }

    /**
     * Returns the explanation chain as a single line (no prefix on the first line).
     */
    @Override
    public String toString() {
        return toString(false);
    }

    /**
     * Returns the explanation chain as a String, optionally prefixing the first line.
     *
     * @param prefixFirstLine whether to prefix the first line as well
     * @return the formatted explanation chain
     */
    public String toString(boolean prefixFirstLine) {
        return toString("caused by: ", prefixFirstLine);
    }

    /**
     * Returns the explanation chain as a String with a custom line prefix.
     * Each cause is placed on a new line, prefixed with {@code linePrefix}. The first
     * line is only prefixed when {@code prefixFirstLine} is {@code true}.
     *
     * @param linePrefix       the prefix to prepend to each line
     * @param prefixFirstLine  whether to prefix the first line as well
     * @return the formatted explanation chain
     */
    public String toString(String linePrefix, boolean prefixFirstLine) {
        if (isAssignable()) {
            return "ASSIGNABLE";
        }

        final var builder = new StringBuilder();
        for (var i = this; i != null; i = i.cause) {
            if (i != this) builder.append("\n");
            if (i != this || prefixFirstLine) builder.append(linePrefix);
            builder.append(i.message);
        }
        return builder.toString();
    }
}
