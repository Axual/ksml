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
 * Represents the result of a deep equality comparison performed via {@link DataEquals#equals(Object, EqualityFlags)}.
 * <p>
 * The result is one of two states:
 * <ul>
 *   <li>{@link Equal} — the compared objects are considered equal.</li>
 *   <li>{@link NotEqual} — the objects are not equal, accompanied by a human-readable explanation, optionally with a cause chain.</li>
 * </ul>
 * Equality failures can be chained using a cause, enabling precise, hierarchical explanations of why equality does not hold.
 * A {@link NotEqual}'s cause can only ever be another {@link NotEqual}, so an equal result carrying a cause, or a failure
 * "caused by" a success, are unrepresentable rather than merely disallowed at runtime.
 */
public sealed interface Equality {
    /**
     * Returns the singleton instance representing a successful equality check (no differences).
     *
     * @return the OK Equal instance
     */
    static Equality equal() {
        return Equal.INSTANCE;
    }

    /**
     * Creates a non-equal result with a human-readable message.
     *
     * @param message explanation of why the objects are not equal
     * @return a new Equality instance representing inequality
     */
    static Equality notEqual(String message) {
        return notEqual(message, null);
    }

    /**
     * Creates a non-equal result with a message and an optional underlying cause.
     *
     * <p>A cause that is itself an {@link Equal} is discarded: chaining a successful comparison as the
     * "cause" of a failed one is meaningless.</p>
     *
     * @param message explanation of why the objects are not equal
     * @param cause   an optional underlying reason providing more detail
     * @return a new Equality instance representing inequality
     * @throws NullPointerException if {@code message} is {@code null}
     */
    static Equality notEqual(@NonNull String message, Equality cause) {
        return new NotEqual(message, cause instanceof NotEqual notEqualCause ? notEqualCause : null);
    }

    /**
     * Indicates whether the compared objects are equal.
     *
     * @return {@code true} if equal; {@code false} otherwise
     */
    boolean isEqual();

    /**
     * Indicates whether the compared objects are not equal.
     *
     * @return {@code true} if not equal; {@code false} otherwise
     */
    boolean isNotEqual();

    /**
     * A human-readable explanation of the inequality, or {@code null} when equal.
     *
     * @return the explanation, or {@code null}
     */
    String message();

    /**
     * The underlying reason for this result, or {@code null} when equal or when there is no further cause.
     *
     * @return the cause, or {@code null}
     */
    Equality cause();

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
     * The successful equality result. A single shared instance, since there is nothing to distinguish
     * between successful comparisons.
     */
    final class Equal implements Equality {
        private static final Equal INSTANCE = new Equal();

        private Equal() {
        }

        @Override
        public boolean isEqual() {
            return true;
        }

        @Override
        public boolean isNotEqual() {
            return false;
        }

        @Override
        public String message() {
            return null;
        }

        @Override
        public Equality cause() {
            return null;
        }

        @Override
        public String toString() {
            return "EQUAL";
        }

        @Override
        public String toString(boolean prefixFirstLine) {
            return "EQUAL";
        }

        @Override
        public String toString(String linePrefix, boolean prefixFirstLine) {
            return "EQUAL";
        }
    }

    /**
     * A failed equality result, carrying a human-readable explanation and an optional chain of
     * underlying causes. A cause, when present, is itself always a {@link NotEqual} — an {@link Equal}
     * can never be the cause of a failure.
     */
    record NotEqual(String message, NotEqual cause) implements Equality {
        @Override
        public boolean isEqual() {
            return false;
        }

        @Override
        public boolean isNotEqual() {
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
