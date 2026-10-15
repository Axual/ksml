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
 *   <li>{@link Ok} — the source type/value can be assigned to the target type/value.</li>
 *   <li>{@link NotAssignable} — the assignment is invalid, accompanied by a human-readable explanation, optionally with a cause chain.</li>
 * </ul>
 * Failures can be chained using a cause, enabling hierarchical explanations of why assignability does not hold.
 * A {@link NotAssignable}'s cause can only ever be another {@link NotAssignable}, so an OK result carrying a cause,
 * or a failure "caused by" a success, are unrepresentable rather than merely disallowed at runtime.
 */
public sealed interface Assignable {
    /**
     * Returns the singleton instance representing a successful assignability check.
     *
     * @return the OK Assignable instance
     */
    static Assignable assignable() {
        return Ok.INSTANCE;
    }

    /**
     * Creates a not-assignable result with a human-readable message.
     *
     * @param message explanation of why the assignment is invalid
     * @return a new Assignable instance representing a failure
     */
    static Assignable notAssignable(String message) {
        return notAssignable(message, null);
    }

    /**
     * Creates a not-assignable result with a message and an optional underlying cause.
     *
     * <p>A cause that is itself an {@link Ok} is discarded: chaining a successful assignability
     * check as the "cause" of a failed one is meaningless.</p>
     *
     * @param message explanation of why the assignment is invalid
     * @param cause   an optional underlying reason providing more detail
     * @return a new Assignable instance representing a failure
     * @throws NullPointerException if {@code message} is {@code null}
     */
    static Assignable notAssignable(@NonNull String message, Assignable cause) {
        return new NotAssignable(message, cause instanceof NotAssignable notAssignableCause ? notAssignableCause : null);
    }

    /**
     * Indicates whether assignment is allowed.
     *
     * @return {@code true} if assignment is allowed; {@code false} otherwise
     */
    boolean isAssignable();

    /**
     * Indicates whether assignment is not allowed.
     *
     * @return {@code true} if assignment is not allowed; {@code false} otherwise
     */
    boolean isNotAssignable();

    /**
     * A human-readable explanation of why assignment is invalid, or {@code null} when assignable.
     *
     * @return the explanation, or {@code null}
     */
    String message();

    /**
     * The underlying reason for this result, or {@code null} when assignable or when there is no further cause.
     *
     * @return the cause, or {@code null}
     */
    Assignable cause();

    /**
     * Returns the explanation chain as a single line (no prefix on the first line).
     */
    @Override
    String toString();

    /**
     * Returns the explanation chain as a String, optionally prefixing the first line.
     *
     * @param prefixFirstLine whether to prefix the first line as well
     * @return the formatted explanation chain
     */
    String toString(boolean prefixFirstLine);

    /**
     * Returns the explanation chain as a String with a custom line prefix.
     * Each cause is placed on a new line, prefixed with {@code linePrefix}. The first
     * line is only prefixed when {@code prefixFirstLine} is {@code true}.
     *
     * @param linePrefix      the prefix to prepend to each line
     * @param prefixFirstLine whether to prefix the first line as well
     * @return the formatted explanation chain
     */
    String toString(String linePrefix, boolean prefixFirstLine);

    /**
     * The successful assignability result. A single shared instance, since there is nothing to
     * distinguish between successful checks.
     */
    final class Ok implements Assignable {
        private static final Ok INSTANCE = new Ok();

        private Ok() {
        }

        @Override
        public boolean isAssignable() {
            return true;
        }

        @Override
        public boolean isNotAssignable() {
            return false;
        }

        @Override
        public String message() {
            return null;
        }

        @Override
        public Assignable cause() {
            return null;
        }

        @Override
        public String toString() {
            return "ASSIGNABLE";
        }

        @Override
        public String toString(boolean prefixFirstLine) {
            return "ASSIGNABLE";
        }

        @Override
        public String toString(String linePrefix, boolean prefixFirstLine) {
            return "ASSIGNABLE";
        }
    }

    /**
     * A failed assignability result, carrying a human-readable explanation and an optional chain of
     * underlying causes. A cause, when present, is itself always a {@link NotAssignable} — an
     * {@link Ok} can never be the cause of a failure.
     */
    record NotAssignable(String message, NotAssignable cause) implements Assignable {
        @Override
        public boolean isAssignable() {
            return false;
        }

        @Override
        public boolean isNotAssignable() {
            return true;
        }

        @Override
        public String toString() {
            return toString(false);
        }

        @Override
        public String toString(boolean prefixFirstLine) {
            return toString("caused by: ", prefixFirstLine);
        }

        @Override
        public String toString(String linePrefix, boolean prefixFirstLine) {
            final var builder = new StringBuilder();
            if (prefixFirstLine) builder.append(linePrefix);
            builder.append(message);
            if (cause != null) cause.appendCauseChain(builder, linePrefix);
            return builder.toString();
        }

        private void appendCauseChain(StringBuilder builder, String linePrefix) {
            builder.append('\n').append(linePrefix).append(message);
            if (cause != null) cause.appendCauseChain(builder, linePrefix);
        }
    }
}
