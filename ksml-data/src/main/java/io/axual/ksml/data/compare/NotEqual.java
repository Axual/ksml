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
import lombok.RequiredArgsConstructor;

/**
 * A failed {@link Equality} result, carrying a human-readable explanation and an optional chain of
 * underlying causes. A cause, when present, is itself always a {@link NotEqual} — an {@link Equal}
 * can never be the cause of a failure.
 * <p>
 * Not a record: {@code Equality} is an abstract class (not an interface, for binary-compatibility
 * reasons — see its javadoc), and records cannot extend a class. {@code message()}/{@code cause()}
 * come from {@code @Getter} (fluent accessors, per the project-wide Lombok config) and the
 * constructor's null-check on {@code message} comes from {@code @NonNull} + {@code @RequiredArgsConstructor}.
 * No {@code @EqualsAndHashCode}/{@code @ToString} on purpose: this keeps the original reference-identity
 * semantics {@code Equality} always had, and {@code toString()} needs custom recursive formatting anyway.
 */
@Getter
@RequiredArgsConstructor
final class NotEqual extends Equality {
    @NonNull
    private final String message;
    private final NotEqual cause;

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