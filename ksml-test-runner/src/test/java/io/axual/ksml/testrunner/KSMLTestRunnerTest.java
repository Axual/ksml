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

import org.graalvm.home.Version;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for {@link KSMLTestRunner}. Requires GraalVM for Python execution.
 */
@EnabledIf(value = "isRunningOnGraalVM", disabledReason = "Requires GraalVM")
class KSMLTestRunnerTest {

    static boolean isRunningOnGraalVM() {
        return Version.getCurrent().isRelease();
    }

    private Path resource(String name) {
        var url = getClass().getClassLoader().getResource(name);
        assertNotNull(url, "Test resource not found: " + name);
        return Path.of(url.getPath());
    }

    @Test
    void confluentAvroFilterTestPasses() {
        var runner = new KSMLTestRunner();
        var results = runner.runTestFile(resource("sample-filter-test-confluent-avro.yaml"));
        assertAllPass(results, "sample-filter-test-confluent-avro.yaml");
    }

    @Test
    void apicurioAvroFilterTestPasses() {
        var runner = new KSMLTestRunner();
        var results = runner.runTestFile(resource("sample-filter-test-apicurio-avro.yaml"));
        assertAllPass(results, "sample-filter-test-apicurio-avro.yaml");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "missing-tests.yaml",
            "missing-produce.yaml",
            "assert-no-topic-no-stores.yaml",
            "invalid-test-key.yaml",
            "invalid-stream-key.yaml",
            "duplicate-test-key.yaml",
            "duplicate-stream-topic.yaml",
            "undefined-stream-reference.yaml",
            "bare-vendor-avro.yaml"
    })
    void invalidDefinitionsReturnNonPass(String testFile) {
        var runner = new KSMLTestRunner();
        var results = runner.runTestFile(resource(testFile));

        assertFalse(results.isEmpty(),
                () -> "Expected at least one result for invalid file '" + testFile + "'");
        assertTrue(results.stream().anyMatch(r -> r.status() != TestResult.Status.PASS),
                () -> "Expected at least one non-PASS result for invalid file '" + testFile
                        + "', got: " + results);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "sample-filter-test.yaml",
            "sample-filter-test-confluent-avro.yaml",
            "sample-filter-test-apicurio-avro.yaml",
            "sample-filter-test-registry-only-types.yaml",
            "sample-filter-test-avro-python-produce.yaml",
            "sample-filter-test-python-produce.yaml",
            "sample-filter-test-module-import.yaml",
            "sample-state-store-test.yaml",
            "sample-timestamp-test.yaml",
            "processor-filtering-transforming-complete-test.yaml"
    })
    void validTestsReturnPass(String testFile) {
        var runner = new KSMLTestRunner();
        var results = runner.runTestFile(resource(testFile));
        assertAllPass(results, testFile);
    }

    @Test
    void suiteWithFailingAssertReportsFailNotError() {
        var runner = new KSMLTestRunner();
        var results = runner.runTestFile(resource("partial-fail-test.yaml"));

        // The fixture has 5 tests; one assertion in `out_of_range_temperature_is_filtered`
        // is inverted to FAIL. The other 4 tests still PASS, demonstrating that the runner
        // does not short-circuit when one test in a suite fails.
        assertEquals(5, results.size(),
                () -> "Expected 5 results, got " + results.size() + ": " + results);

        long passes = results.stream().filter(r -> r.status() == TestResult.Status.PASS).count();
        long fails = results.stream().filter(r -> r.status() == TestResult.Status.FAIL).count();
        long errors = results.stream().filter(r -> r.status() == TestResult.Status.ERROR).count();

        assertEquals(0, errors, () -> "Expected zero ERROR results, got " + errors + ": " + results);
        assertEquals(1, fails, () -> "Expected exactly one FAIL result, got " + fails + ": " + results);
        assertEquals(4, passes, () -> "Expected exactly four PASS results, got " + passes + ": " + results);

        // The failing test is the second one in YAML order; verify its identity and that the
        // failure message comes from the assertion (FAIL), not from a runtime exception (ERROR).
        var failed = results.stream()
                .filter(r -> r.status() == TestResult.Status.FAIL)
                .findFirst()
                .orElseThrow();
        assertTrue(failed.testName().contains("Out-of-range") || failed.testName().contains("out_of_range"),
                () -> "Expected the FAIL to be the out-of-range test, got: " + failed.testName());
        assertNotNull(failed.message());
        assertTrue(failed.message().contains("AssertionError"),
                () -> "Expected FAIL message to mention AssertionError, got: " + failed.message());
    }

    private static void assertAllPass(List<TestResult> results, String testFile) {
        assertFalse(results.isEmpty(), () -> "Expected at least one result for '" + testFile + "'");
        for (var result : results) {
            assertEquals(TestResult.Status.PASS, result.status(),
                    () -> "Expected PASS for '" + testFile + "' (" + result.qualifiedLabel() + ") but got "
                            + result.status() + ": " + result.message());
        }
    }
}
