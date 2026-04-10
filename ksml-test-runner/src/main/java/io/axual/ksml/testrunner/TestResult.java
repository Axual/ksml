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
 * Result of executing a single test definition.
 *
 * @param testName the human-readable test name
 * @param status   PASS, FAIL, or ERROR
 * @param message  details on failure/error, or null for PASS
 */
public record TestResult(
        String testName,
        Status status,
        String message
) {
    public enum Status {
        PASS, FAIL, ERROR
    }

    public static TestResult pass(String testName) {
        return new TestResult(testName, Status.PASS, null);
    }

    public static TestResult fail(String testName, String message) {
        return new TestResult(testName, Status.FAIL, message);
    }

    public static TestResult error(String testName, String message) {
        return new TestResult(testName, Status.ERROR, message);
    }
}
