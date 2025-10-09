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
 * Represents the result of a deep equality comparison performed via {@link DataEquals#equals(Object, io.axual.ksml.data.type.Flags)}.
 * <p>
 * The result can be in two states:
 * <ul>
 *   <li>OK — the compared objects are considered equal.</li>
 *   <li>Not OK — the objects are not equal, accompanied by a human-readable explanation, optionally with a cause chain.</li>
 * </ul>
 * Equality failures can be chained using a cause, enabling precise, hierarchical explanations of why equality does not hold.
 * To optimize for the common success case, the OK state is implemented as a singleton instance (message is {@code null}).
 */
@Getter
public class Equal {
    private static final Equal OK = new Equal(null, null);
    private final String message;
    private final Equal cause;

    /**
     * Creates a new Equal result.
     *
     * @param message a human-readable explanation of the inequality; {@code null} means the objects are equal
     * @param cause   an optional underlying reason; only stored when it represents a non-equal state
     */
    private Equal(String message, Equal cause) {
        this.message = message;
        this.cause = cause != null && cause.isNotEqual() ? cause : null;
    }

    /**
     * Returns the singleton instance representing a successful equality check (no differences).
     *
     * @return the OK Equal instance
     */
    public static Equal ok() {
        return OK;
    }

    /**
     * Creates a non-equal result with a human-readable message.
     *
     * @param message explanation of why the objects are not equal
     * @return a new Equal instance representing inequality
     */
    public static Equal notEqual(String message) {
        return notEqual(message, null);
    }

    /**
     * Creates a non-equal result with a message and an optional underlying cause.
     *
     * @param message explanation of why the objects are not equal
     * @param cause   an optional underlying reason providing more detail
     * @return a new Equal instance representing inequality
     * @throws NullPointerException if {@code message} is {@code null}
     */
    public static Equal notEqual(String message, Equal cause) {
        Objects.requireNonNull(message, "message must not be null");
        return new Equal(message, cause);
    }

    /**
     * Indicates whether the compared objects are equal.
     *
     * @return {@code true} if equal; {@code false} otherwise
     */
    public boolean isEqual() {
        return message == null;
    }

    /**
     * Indicates whether the compared objects are not equal.
     *
     * @return {@code true} if not equal; {@code false} otherwise
     */
    public boolean isNotEqual() {
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
        final var builder = new StringBuilder();
        for (var i = this; i != null; i = i.cause) {
            if (i != this) builder.append("\n");
            if (i != this || prefixFirstLine) builder.append(linePrefix);
            builder.append(i.message);
        }
        return builder.toString();
    }
}
