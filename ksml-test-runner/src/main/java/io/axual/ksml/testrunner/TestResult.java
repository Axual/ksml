package io.axual.ksml.testrunner;

/*-
 * ========================LICENSE_START=================================
 * KSML Test Runner
 * %%
 * Copyright (C) 2021 - 2026 Axual B.V.
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
 * Result of executing a single test entry.
 *
 * @param suiteName the suite-level name (file's {@code name:} or filename without extension)
 * @param testName  the per-test display label (test entry's {@code description:} or test key)
 * @param status    PASS, FAIL, or ERROR
 * @param message   details on failure/error, or null for PASS
 */
public record TestResult(
        String suiteName,
        String testName,
        Status status,
        String message
) {
    public enum Status {
        PASS, FAIL, ERROR
    }

    public static TestResult pass(String suiteName, String testName) {
        return new TestResult(suiteName, testName, Status.PASS, null);
    }

    public static TestResult fail(String suiteName, String testName, String message) {
        return new TestResult(suiteName, testName, Status.FAIL, message);
    }

    public static TestResult error(String suiteName, String testName, String message) {
        return new TestResult(suiteName, testName, Status.ERROR, message);
    }

    /**
     * Compose the qualified display label as {@code <suite> › <test>}.
     */
    public String qualifiedLabel() {
        if (suiteName == null || suiteName.isEmpty()) return testName;
        if (testName == null || testName.isEmpty()) return suiteName;
        return suiteName + " › " + testName;
    }
}
