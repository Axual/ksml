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
 * The successful {@link Equality} result. A single shared instance, since there is nothing to
 * distinguish between successful comparisons.
 */
final class Equal extends Equality {
    static final Equal INSTANCE = new Equal();
    private static final String ISEQUAL = "EQUAL";

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
        return ISEQUAL;
    }

    @Override
    public String toString(boolean prefixFirstLine) {
        return ISEQUAL;
    }

    @Override
    public String toString(String linePrefix, boolean prefixFirstLine) {
        return ISEQUAL;
    }
}