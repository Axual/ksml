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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TestResultTest {

    @Test
    void passResultHasCorrectStatus() {
        var result = TestResult.pass("my test");
        assertEquals("my test", result.testName());
        assertEquals(TestResult.Status.PASS, result.status());
        assertNull(result.message());
    }

    @Test
    void failResultHasMessageAndStatus() {
        var result = TestResult.fail("my test", "expected 2, got 3");
        assertEquals("my test", result.testName());
        assertEquals(TestResult.Status.FAIL, result.status());
        assertEquals("expected 2, got 3", result.message());
    }

    @Test
    void errorResultHasMessageAndStatus() {
        var result = TestResult.error("my test", "NullPointerException");
        assertEquals("my test", result.testName());
        assertEquals(TestResult.Status.ERROR, result.status());
        assertEquals("NullPointerException", result.message());
    }
}
