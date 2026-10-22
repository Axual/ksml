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

import lombok.NonNull;

/**
 * Represents the result of an assignability check (typically used by isAssignableFrom-like methods).
 * <p>
 * The result is one of two states:
 * <ul>
 *   <li>{@link IsAssignable} — the source type/value can be assigned to the target type/value.</li>
 *   <li>{@link NotAssignable} — the assignment is invalid, accompanied by a human-readable explanation, optionally with a cause chain.</li>
 * </ul>
 * Failures can be chained using a cause, enabling hierarchical explanations of why assignability does not hold.
 * A {@link NotAssignable}'s cause can only ever be another {@link NotAssignable}, so an OK result carrying a cause,
 * or a failure "caused by" a success, are unrepresentable rather than merely disallowed at runtime.
 * <p>
 * This is a {@code sealed abstract class} rather than a sealed interface to maintain binary compatibility with older code.
 */
public abstract sealed class Assignable permits IsAssignable, NotAssignable {
    /**
     * Returns the singleton instance representing a successful assignability check.
     *
     * @return the OK Assignable instance
     */
    public static Assignable assignable() {
        return IsAssignable.INSTANCE;
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
     * <p>A cause that is itself an {@link IsAssignable} is discarded: chaining a successful assignability
     * check as the "cause" of a failed one is meaningless.</p>
     *
     * @param message explanation of why the assignment is invalid
     * @param cause   an optional underlying reason providing more detail
     * @return a new Assignable instance representing a failure
     * @throws NullPointerException if {@code message} is {@code null}
     */
    public static Assignable notAssignable(@NonNull String message, Assignable cause) {
        return new NotAssignable(message, cause instanceof NotAssignable notAssignableCause ? notAssignableCause : null);
    }

    /**
     * Indicates whether assignment is allowed.
     *
     * @return {@code true} if assignment is allowed; {@code false} otherwise
     */
    public abstract boolean isAssignable();

    /**
     * Indicates whether assignment is not allowed.
     *
     * @return {@code true} if assignment is not allowed; {@code false} otherwise
     */
    public abstract boolean isNotAssignable();

    /**
     * A human-readable explanation of why assignment is invalid, or {@code null} when assignable.
     *
     * @return the explanation, or {@code null}
     */
    public abstract String message();

    /**
     * The underlying reason for this result, or {@code null} when assignable or when there is no further cause.
     *
     * @return the cause, or {@code null}
     */
    public abstract Assignable cause();

    /**
     * Returns the explanation chain as a single line (no prefix on the first line).
     */
    @Override
    public abstract String toString();

    /**
     * Returns the explanation chain as a String, optionally prefixing the first line.
     *
     * @param prefixFirstLine whether to prefix the first line as well
     * @return the formatted explanation chain
     */
    public abstract String toString(boolean prefixFirstLine);

    /**
     * Returns the explanation chain as a String with a custom line prefix.
     * Each cause is placed on a new line, prefixed with {@code linePrefix}. The first
     * line is only prefixed when {@code prefixFirstLine} is {@code true}.
     *
     * @param linePrefix      the prefix to prepend to each line
     * @param prefixFirstLine whether to prefix the first line as well
     * @return the formatted explanation chain
     */
    public abstract String toString(String linePrefix, boolean prefixFirstLine);
}
