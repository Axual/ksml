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

import java.util.List;

/**
 * The three kinds of Python source text a FunctionDefinition can carry: global setup code,
 * the function body, and a return expression. Bundled into one type so FunctionDefinition's
 * factory methods and constructor take one parameter for "the code" instead of three.
 * <p>
 * Stored internally as {@link List} rather than {@code String[]} so that the record-derived
 * {@code equals()}/{@code hashCode()} work correctly (arrays don't override those methods, so a
 * plain record with array components would need hand-written {@code Arrays.equals}/{@code Arrays.hashCode}
 * overrides instead). {@code of(String[], ...)} converts on the way in; callers that need a
 * {@code String[]} (e.g. {@link FunctionDefinition}) convert on the way out.
 *
 * @param globalCode 1 or more lines of global code that gets loaded into the Python context.
 * @param code 1 or more lines of code that gets executed in the Python context.
 * @param expression 1 line of code that gets executed in the Python context and returned as the result.
 */
public record PythonSource(List<String> globalCode, List<String> code, List<String> expression) {
    public PythonSource(List<String> globalCode, List<String> code, List<String> expression) {
        this.globalCode = globalCode != null ? List.copyOf(globalCode) : List.of();
        this.code = code != null ? List.copyOf(code) : List.of();
        this.expression = expression != null ? List.copyOf(expression) : null;
    }

    public static PythonSource of(String globalCode, String code, String expression) {
        return new PythonSource(multiline(globalCode), multiline(code), multiline(expression));
    }

    public static PythonSource of(String[] globalCode, String[] code, String[] expression) {
        return new PythonSource(toList(globalCode), toList(code), toList(expression));
    }

    private static List<String> multiline(String lines) {
        if (lines == null) return List.of();
        return List.of(lines.split("\\r?\\n"));
    }

    private static List<String> toList(String[] lines) {
        return lines != null ? List.of(lines) : null;
    }

    @Override
    public String toString() {
        return "PythonSource[globalCode=" + describe(globalCode) + ", code=" + describe(code) + ", expression=" + describe(expression) + "]";
    }

    private static String describe(List<String> lines) {
        if (lines == null) return "null";
        return lines.isEmpty() ? "none" : lines.size() + " line(s)";
    }
}
