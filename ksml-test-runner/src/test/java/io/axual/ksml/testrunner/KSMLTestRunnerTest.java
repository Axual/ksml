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
        var result = runner.runSingleTest(resource("sample-filter-test-confluent-avro.yaml"));

        assertEquals(TestResult.Status.PASS, result.status(),
                () -> "Expected PASS but got " + result.status() + ": " + result.message());
    }

    @Test
    void apicurioAvroFilterTestPasses() {
        var runner = new KSMLTestRunner();
        var result = runner.runSingleTest(resource("sample-filter-test-apicurio-avro.yaml"));

        assertEquals(TestResult.Status.PASS, result.status(),
                () -> "Expected PASS but got " + result.status() + ": " + result.message());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "missing-test-root.yaml",
            "missing-name.yaml",
            "missing-produce.yaml",
            "assert-no-topic-no-stores.yaml"
    })
    void invalidDefinitionsReturnNonPass(String testFile) {
        var runner = new KSMLTestRunner();
        var result = runner.runSingleTest(resource(testFile));

        assertNotEquals(TestResult.Status.PASS, result.status(),
                () -> "Expected non-PASS for invalid definition '" + testFile + "' but got PASS");
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
            "sample-timestamp-test.yaml"
    })
    void validTestsReturnPass(String testFile) {
        var runner = new KSMLTestRunner();
        var result = runner.runSingleTest(resource(testFile));

        assertEquals(TestResult.Status.PASS, result.status(),
                () -> "Expected PASS for '" + testFile + "' but got " + result.status() + ": " + result.message());
    }

    @Test
    void filteringTransformingCompleteTestsAllPass() throws Exception {
        var dir = resource("processor-filtering-transforming-complete-test");
        var testFiles = KSMLTestRunner.collectTestFiles(List.of(dir.toFile()));
        assertFalse(testFiles.isEmpty(), () -> "No test files found in " + dir);

        var runner = new KSMLTestRunner();
        for (var file : testFiles) {
            var result = runner.runSingleTest(file);
            assertEquals(TestResult.Status.PASS, result.status(),
                    () -> "Expected PASS for '" + file.getFileName() + "' but got "
                            + result.status() + ": " + result.message());
        }
    }
}
