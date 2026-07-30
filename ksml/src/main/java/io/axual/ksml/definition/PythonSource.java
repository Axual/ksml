package io.axual.ksml.definition;

/*-
 * ========================LICENSE_START=================================
 * KSML
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

// The three kinds of Python source text a FunctionDefinition can carry: global setup code,
// the function body, and a return expression. Bundled into one type so FunctionDefinition's
// factory methods and constructor take one parameter for "the code" instead of three,
// keeping their parameter counts under the java:S107 threshold.

import java.util.Arrays;

public record PythonSource(String[] globalCode, String[] code, String[] expression) {
    private static final String[] EMPTY_LINES = new String[]{};

    public PythonSource(String[] globalCode, String[] code, String[] expression) {
        this.globalCode = globalCode != null ? globalCode : EMPTY_LINES;
        this.code = code != null ? code : EMPTY_LINES;
        this.expression = expression;
    }

    public static PythonSource of(String globalCode, String code, String expression) {
        return new PythonSource(multiline(globalCode), multiline(code), multiline(expression));
    }

    public static PythonSource of(String[] globalCode, String[] code, String[] expression) {
        return new PythonSource(globalCode, code, expression);
    }

    private static String[] multiline(String lines) {
        if (lines == null) return EMPTY_LINES;
        return lines.split("\\r?\\n");
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        PythonSource that = (PythonSource) obj;
        return Arrays.equals(globalCode, that.globalCode)
            && Arrays.equals(code, that.code)
            && Arrays.equals(expression, that.expression);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(globalCode);
        result = 31 * result + Arrays.hashCode(code);
        result = 31 * result + Arrays.hashCode(expression);
        return result;
    }

    @Override
    public String toString() {
        return "PythonSource[globalCode=" + describe(globalCode) + ", code=" + describe(code) + ", expression=" + describe(expression) + "]";
    }

    private static String describe(String[] lines) {
        if (lines == null) return "null";
        return lines.length == 0 ? "none" : lines.length + " line(s)";
    }
}
