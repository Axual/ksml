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

/**
 * The successful {@link Assignable} result. A single shared instance, since there is nothing to
 * distinguish between successful checks.
 */
final class IsAssignable extends Assignable {
    static final IsAssignable INSTANCE = new IsAssignable();
    private static final String ASSIGNABLE = "ASSIGNABLE";

    private IsAssignable() {
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
        return ASSIGNABLE;
    }

    @Override
    public String toString(boolean prefixFirstLine) {
        return ASSIGNABLE;
    }

    @Override
    public String toString(String linePrefix, boolean prefixFirstLine) {
        return ASSIGNABLE;
    }
}